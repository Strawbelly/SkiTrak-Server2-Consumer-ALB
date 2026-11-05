package consumer;

import com.google.gson.Gson;
import com.google.gson.JsonParseException;
import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.DefaultConsumer;
import com.rabbitmq.client.Envelope;
import consumer.model.SwipeEvent;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;

/**
 * Processes messages from the resort queues assigned to this worker.
 *
 * Each worker owns a RabbitMQ Channel and one processing thread.
 * RabbitMQ callbacks enqueue deliveries locally, while the worker thread processes them sequentially, updates the leaderboard, and acknowledges or rejects messages.
 *
 * If a message fails, retries block this worker until the retry finishes.
 */
final class ResortWorker {

    private static final Logger LOG =
            Logger.getLogger(ResortWorker.class.getName());

    private static final Gson GSON = new Gson();

    // Leaderboard updates are transient because each message contains a full snapshot.
    private static final AMQP.BasicProperties LEADERBOARD_PROPS =
            new AMQP.BasicProperties.Builder()
                    .contentType("application/json")
                    .deliveryMode(1)
                    .build();

    private final int index;
    private final Channel channel;
    private final LeaderboardAggregator aggregator;
    private final ConsumerPool pool;

    // Processed deliveries acknowledged together (1 = each one), and the pending batch.
    // Both are used only by the worker thread, which owns this channel.
    private final int ackBatch;
    private int pendingAcks;
    private long lastPendingTag;

    // Maximum processing attempts and the initial retry delay.
    private final int maxAttempts;
    private final long retryBackoffMs;

    // Buffers delivery tasks received from RabbitMQ callbacks.
    private final BlockingQueue<Runnable> deliveries =
            new LinkedBlockingQueue<>();

    private final Thread thread;

    // Volatile makes shutdown changes visible to the worker thread.
    private volatile boolean running = true;

    ResortWorker(
            int index,
            Connection connection,
            int prefetch,
            int ackBatch,
            int maxAttempts,
            long retryBackoffMs,
            LeaderboardAggregator aggregator,
            ConsumerPool pool) throws IOException {

        this.index = index;
        this.aggregator = aggregator;
        this.pool = pool;
        this.ackBatch = ackBatch;
        this.maxAttempts = maxAttempts;
        this.retryBackoffMs = retryBackoffMs;

        // Each worker creates and owns its own channel.
        this.channel = connection.createChannel();

        // Limits the number of unacknowledged messages delivered to each consumer.
        channel.basicQos(prefetch);

        // Create the processing thread; it starts when start() is called.
        this.thread = new Thread(this::run, "resort-worker-" + index);
    }

    /**
     * Subscribes to a resort queue with manual acknowledgments.
     * Called before start().
     */
    void subscribe(int resortID) throws IOException {
        channel.basicConsume(
                Topology.resortQueue(resortID),
                false, // Disable automatic ACK; messages are acknowledged manually.
                new DefaultConsumer(channel) {
                    @Override
                    public void handleDelivery(
                            String tag,
                            Envelope envelope,
                            AMQP.BasicProperties props,
                            byte[] body) {

                        // Delivery tags identify messages on this channel.
                        long deliveryTag = envelope.getDeliveryTag();
                        Long publishMs = publishMs(props);

                        // Defer business processing to the worker's own thread.
                        deliveries.add(
                                () -> handle(resortID, deliveryTag, body, publishMs));
                    }
                });
    }

    /** Starts the worker's processing thread. */
    void start() {
        thread.start();
    }

    /** Returns true while the processing thread is running and the channel is open. */
    boolean isHealthy() {
        return thread.isAlive() && channel.isOpen();
    }

    /**
     * Stops the worker and closes its channel.
     * Unacknowledged messages are eligible for redelivery when the channel closes.
     */
    void stop() {
        running = false;

        try {
            // Allow the current task to finish before forcing interruption.
            thread.join(10_000);

            if (thread.isAlive()) {
                thread.interrupt(); // Also interrupts a retry backoff sleep.
                thread.join(2_000);
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }

        try {
            // Closing the channel allows RabbitMQ to requeue unacknowledged deliveries.
            channel.close();
        } catch (Exception ignored) {
            // The connection or channel may already be closed.
        }
    }

    /**
     * Continuously takes delivery tasks from the local queue.
     * Only one task is processed at a time by this worker thread.
     */
    private void run() {
        try {
            while (running) {
                // Wait up to 200 ms for a task when the queue is empty.
                Runnable delivery = deliveries.poll(200, TimeUnit.MILLISECONDS);

                if (delivery != null) {
                    delivery.run();
                }
                // Never hold a partial batch while idle: with prefetch reached, RabbitMQ
                // sends nothing more until these deliveries are acknowledged.
                if (deliveries.isEmpty()) {
                    flushAcks();
                }
            }
        } catch (InterruptedException e) {
            // Stopping; fall through to acknowledge what was already processed.
        } finally {
            // stop() closes the channel only after this thread ends, so this still reaches RabbitMQ.
            flushAcks();
        }
    }

    /**
     * Parses a swipe, updates the leaderboard, and acknowledges the message.
     * Transient processing failures are retried with exponential backoff.
     */
    private void handle(int resortID, long deliveryTag, byte[] body, Long publishMs) {
        // Consumer lag: processing start minus the ingest server's publish time.
        pool.lag.record(publishMs, System.currentTimeMillis());

        SwipeEvent event;

        try {
            event = parse(resortID, body);
        } catch (InvalidEventException e) {
            // Invalid messages cannot be processed successfully; send them to the DLQ.
            LOG.warning("dead-lettering invalid message on resort "
                    + resortID + ": " + e.getMessage());
            reject(deliveryTag);
            return;
        }

        for (int attempt = 1; ; attempt++) {
            try {
                // Aggregate this event and determine whether the standings changed.
                LeaderboardAggregator.Outcome outcome = aggregator.apply(event);

                if (outcome == LeaderboardAggregator.Outcome.DUPLICATE) {
                    pool.duplicates.incrementAndGet();
                }

                // Broadcast a fresh full snapshot only when the leaderboard changes.
                if (outcome == LeaderboardAggregator.Outcome.CHANGED) {
                    broadcast(event.getResortID(), event.getDayID());
                }

                // Acknowledge the swipe after aggregation and the broadcast attempt.
                ack(deliveryTag);
                return;

            } catch (RuntimeException e) {
                if (attempt >= maxAttempts) {
                    LOG.warning("dead-lettering " + event.getEventId()
                            + " after " + attempt + " attempts: " + e);
                    reject(deliveryTag);
                    return;
                }

                pool.retries.incrementAndGet();

                try {
                    // Exponential backoff: base delay * 2^(attempt - 1).
                    Thread.sleep(retryBackoffMs << (attempt - 1));
                } catch (InterruptedException ie) {
                    // Leave the message unacknowledged for possible broker redelivery.
                    Thread.currentThread().interrupt();
                    return;
                }
            }
        }
    }

    /**
     * Publishes a full leaderboard snapshot to the fanout exchange.
     * Best effort: a later leaderboard change publishes another full snapshot.
     */
    private void broadcast(int resortID, int dayID) {
        try {
            byte[] json = GSON.toJson(
                            aggregator.snapshot(resortID, dayID))
                    .getBytes(StandardCharsets.UTF_8);

            // Fanout routes the update to bound queues, such as server relay queues.
            channel.basicPublish(
                    Topology.LEADERBOARD_EXCHANGE,
                    "",
                    LEADERBOARD_PROPS,
                    json);

            pool.broadcasts.incrementAndGet();

        } catch (IOException | RuntimeException e) {
            // A broadcast failure is logged; it does not currently prevent the swipe ACK.
            LOG.warning("leaderboard broadcast failed for resort "
                    + resortID + ": " + e);
        }
    }

    /**
     * Acknowledges a successfully processed delivery, either alone or as part of a batch.
     * Deliveries are processed in tag order on this channel, so a multiple-ack up to the
     * last processed tag covers only processed deliveries.
     */
    private void ack(long deliveryTag) {
        if (ackBatch == 1) {
            basicAck(deliveryTag, false);
            return;
        }
        lastPendingTag = deliveryTag;
        if (++pendingAcks >= ackBatch) {
            flushAcks();
        }
    }

    /** Acknowledges every processed delivery up to the last pending tag. */
    private void flushAcks() {
        if (pendingAcks > 0) {
            basicAck(lastPendingTag, true);
            pendingAcks = 0;
        }
    }

    private void basicAck(long deliveryTag, boolean multiple) {
        try {
            channel.basicAck(deliveryTag, multiple);
        } catch (IOException | RuntimeException e) {
            // If ACK fails, the message may be redelivered; eventId deduplication helps.
            pool.ackFailures.incrementAndGet();
        }
    }

    /** Rejects a delivery without requeueing it, allowing configured DLX routing. */
    private void reject(long deliveryTag) {
        // Settle the processed deliveries before this one.
        flushAcks();
        try {
            // With the queue's DLX configured, the rejected message can reach the DLQ.
            channel.basicReject(deliveryTag, false);
            pool.deadLettered.incrementAndGet();
        } catch (IOException | RuntimeException e) {
            pool.ackFailures.incrementAndGet();
        }
    }

    /** Reads the ingest server's publish time from the message headers, if present. */
    static Long publishMs(AMQP.BasicProperties props) {
        Map<String, Object> headers = props == null ? null : props.getHeaders();
        Object v = headers == null ? null : headers.get(LagStats.HEADER);
        return v instanceof Number ? ((Number) v).longValue() : null;
    }

    /**
     * Parses and validates a swipe message.
     * Throws InvalidEventException for malformed or invalid business data.
     */
    static SwipeEvent parse(int queueResortID, byte[] body) {
        SwipeEvent e;

        try {
            e = GSON.fromJson(
                    new String(body, StandardCharsets.UTF_8),
                    SwipeEvent.class);
        } catch (JsonParseException ex) {
            throw new InvalidEventException(
                    "malformed JSON: " + ex.getMessage());
        }

        if (e == null || e.getEventId() == null || e.getEventId().isBlank()) {
            throw new InvalidEventException("missing eventId");
        }

        // Ensure the message was routed to the queue for its own resort.
        if (e.getResortID() != queueResortID) {
            throw new InvalidEventException(
                    "resortID " + e.getResortID()
                            + " on queue for resort " + queueResortID);
        }

        if (e.getDayID() < 1
                || e.getSkierID() < 1
                || e.getLiftID() < 1
                || e.getVertical() < 1) {
            throw new InvalidEventException(
                    "non-positive field in " + e.getEventId());
        }

        return e;
    }

    /** Indicates that a message is malformed or permanently invalid. */
    static final class InvalidEventException extends RuntimeException {
        InvalidEventException(String message) {
            super(message);
        }
    }
}
