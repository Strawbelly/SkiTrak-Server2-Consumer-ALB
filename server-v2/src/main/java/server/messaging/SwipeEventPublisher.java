package server.messaging;

import com.google.gson.Gson;
import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.BuiltinExchangeType;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.ShutdownSignalException;
import server.model.SwipeEvent;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.concurrent.TimeoutException;

/**
 * Publishes {@link SwipeEvent}s as JSON to {@value #EXCHANGE} with routing key
 * {@code resort.{resortID}}, using channels borrowed from a {@link ChannelPool}.
 */
public class SwipeEventPublisher {
    public static final String EXCHANGE = "skitrak.exchange";
    public static final String ROUTING_KEY_PREFIX = "resort.";
    /** Header carrying the publish time in epoch ms (read by the consumer's LagStats). */
    public static final String PUBLISH_MS_HEADER = "x-publish-ms";
    static final int MAX_ATTEMPTS = 2;

    private static final Gson GSON = new Gson();

    private final ChannelPool pool;
    private final boolean publisherConfirms;
    private final long confirmTimeoutMs;

    public SwipeEventPublisher(ChannelPool pool, boolean publisherConfirms, long confirmTimeoutMs) {
        this.pool = pool;
        this.publisherConfirms = publisherConfirms;
        this.confirmTimeoutMs = confirmTimeoutMs;
    }

    /**
     * Sets up each new pooled channel: enables publisher confirms (if configured) and
     * declares the durable topic exchange, so a reconnect to a fresh broker recreates it.
     */
    public static ChannelPool.ChannelInitializer channelInitializer(boolean publisherConfirms) {
        return channel -> {
            if (publisherConfirms) {
                channel.confirmSelect();
            }
            channel.exchangeDeclare(EXCHANGE, BuiltinExchangeType.TOPIC, true);
        };
    }

    public static String routingKey(int resortID) {
        return ROUTING_KEY_PREFIX + resortID;
    }

    /**
     * Publishes one event, retrying once on a fresh channel if the first attempt fails.
     * With confirms enabled, returns only after the broker has accepted the message.
     * A retry after a lost confirm can duplicate a message; consumers can dedupe on eventId.
     *
     * @throws IOException if the event could not be published
     */
    public void publish(SwipeEvent event) throws IOException, InterruptedException {
        byte[] body = GSON.toJson(event).getBytes(StandardCharsets.UTF_8);
        String routingKey = routingKey(event.getResortID());
        AMQP.BasicProperties props = new AMQP.BasicProperties.Builder()
                .contentType("application/json")
                .contentEncoding("UTF-8")
                .deliveryMode(2) // persistent
                .messageId(event.getEventId())
                // Publish time in epoch ms, used by the consumer to measure lag. AMQP's own
                // timestamp property has only one-second resolution.
                .headers(Map.of(PUBLISH_MS_HEADER, System.currentTimeMillis()))
                .build();

        IOException lastFailure = null;
        for (int attempt = 1; attempt <= MAX_ATTEMPTS; attempt++) {
            Channel channel = pool.borrowChannel();
            try {
                channel.basicPublish(EXCHANGE, routingKey, props, body);
                if (publisherConfirms) {
                    channel.waitForConfirmsOrDie(confirmTimeoutMs);
                }
                return;
            } catch (IOException | TimeoutException | ShutdownSignalException e) {
                lastFailure = e instanceof IOException ? (IOException) e
                        : new IOException("publish failed: " + e.getMessage(), e);
                // Channel state is unknown after a failure; closing it makes the pool replace it.
                ChannelPool.closeQuietly(channel);
            } finally {
                pool.returnChannel(channel);
            }
        }
        throw lastFailure;
    }
}
