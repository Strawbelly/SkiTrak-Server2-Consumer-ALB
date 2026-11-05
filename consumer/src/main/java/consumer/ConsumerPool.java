package consumer;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.Logger;

/**
 * Manages a resizable pool of ResortWorkers.
 * Resorts are assigned round-robin for balanced distribution.
 * Resizing stops all workers before reassigning resorts, causing a brief
 * pause in consumption.
 */
public class ConsumerPool implements AutoCloseable {

    private static final Logger LOG =
            Logger.getLogger(ConsumerPool.class.getName());

    // Shared worker statistics.
    final AtomicLong duplicates = new AtomicLong();
    final AtomicLong retries = new AtomicLong();
    final AtomicLong deadLettered = new AtomicLong();
    final AtomicLong ackFailures = new AtomicLong();
    final AtomicLong broadcasts = new AtomicLong();
    final LagStats lag = new LagStats();

    private final Connection connection;
    private final ConsumerConfig config;
    private final LeaderboardAggregator aggregator;

    // Access guarded by synchronized methods.
    private final List<ResortWorker> workers = new ArrayList<>();

    // Periodically adjusts the pool size based on queue backlog.
    private final ScheduledExecutorService autoscaler =
            Executors.newSingleThreadScheduledExecutor(r -> {
                Thread t = new Thread(r, "consumer-autoscaler");
                t.setDaemon(true);
                return t;
            });

    public ConsumerPool(Connection connection, ConsumerConfig config,
                        LeaderboardAggregator aggregator) throws IOException {
        this.connection = connection;
        this.config = config;
        this.aggregator = aggregator;

        try (Channel ch = connection.createChannel()) {
            Topology.declare(ch, config.resortIds);
        } catch (java.util.concurrent.TimeoutException e) {
            throw new IOException(e);
        }
    }

    /** Resizes the pool and reassigns resort queues. */
    public synchronized void resize(int n) throws IOException {
        int target = Math.max(config.minWorkers,
                Math.min(config.maxWorkers, n));

        if (target == workers.size()) {
            return;
        }

        int before = workers.size();

        // Stop existing workers before changing resort assignments.
        for (ResortWorker w : workers) {
            w.stop();
        }
        workers.clear();

        for (int i = 0; i < target; i++) {
            workers.add(new ResortWorker(i, connection, config.prefetch, config.ackBatch,
                    config.maxAttempts, config.retryBackoffMs, aggregator, this));
        }

        Map<Integer, List<Integer>> assignment = assign(config.resortIds, target);

        for (Map.Entry<Integer, List<Integer>> e : assignment.entrySet()) {
            for (int resortID : e.getValue()) {
                workers.get(e.getKey()).subscribe(resortID);
            }
        }

        for (ResortWorker w : workers) {
            w.start();
        }

        LOG.info("consumer threads " + before + " -> " + target
                + ", resorts per thread " + assignment);
    }

    public synchronized int size() {
        return workers.size();
    }

    /**
     * Returns false if a worker thread has died or its channel was closed while the
     * connection is open. A closed connection is not counted as a failure because
     * automatic recovery restores it and its channels.
     */
    public synchronized boolean workersHealthy() {
        if (!connection.isOpen()) {
            return true;
        }
        for (ResortWorker w : workers) {
            if (!w.isHealthy()) {
                return false;
            }
        }
        return !workers.isEmpty();
    }

    /** Periodically scales the pool according to the queue backlog. */
    public void startAutoscaler() {
        autoscaler.scheduleWithFixedDelay(() -> {
                    try {
                        int n = size();
                        int desired = desiredSize(backlog(), n,
                                config.scaleUpBacklogPerWorker,
                                config.scaleDownBacklog);

                        if (desired != n) {
                            resize(desired);
                        }
                    } catch (IOException | RuntimeException e) {
                        LOG.warning("autoscale check failed: " + e);
                    }
                }, config.autoscaleIntervalMs, config.autoscaleIntervalMs,
                TimeUnit.MILLISECONDS);
    }

    /** Returns total ready messages, or -1 if the backlog cannot be read. */
    public long backlog() {
        try (Channel ch = connection.createChannel()) {
            long total = 0;
            for (int resortID : config.resortIds) {
                total += ch.queueDeclarePassive(
                        Topology.resortQueue(resortID)).getMessageCount();
            }
            return total;
        } catch (Exception e) {
            return -1;
        }
    }

    @Override
    public synchronized void close() {
        autoscaler.shutdownNow();
        for (ResortWorker w : workers) {
            w.stop();
        }
        workers.clear();
    }

    /**
     * Maps each worker index to the resort IDs it consumes, round-robin over
     * max(resorts, workers) slots. Every worker and every resort gets at least one
     * slot, and per-worker and per-resort counts differ by at most one. With more
     * workers than resorts, each resort queue has several subscribers: with
     * single-active-consumer, one is active and the others are standbys.
     */
    static Map<Integer, List<Integer>> assign(List<Integer> resortIds, int workers) {
        Map<Integer, List<Integer>> assignment = new TreeMap<>();
        int slots = Math.max(resortIds.size(), workers);
        for (int k = 0; k < slots; k++) {
            assignment.computeIfAbsent(k % workers, w -> new ArrayList<>())
                    .add(resortIds.get(k % resortIds.size()));
        }
        return assignment;
    }

    /** Adds one worker above the scale-up threshold or removes one below the scale-down threshold. */
    static int desiredSize(long backlog, int workers, int upPerWorker, int down) {
        if (backlog < 0) {
            return workers;
        }
        if (backlog > (long) upPerWorker * workers) {
            return workers + 1;
        }
        if (backlog < down) {
            return workers - 1;
        }
        return workers;
    }
}