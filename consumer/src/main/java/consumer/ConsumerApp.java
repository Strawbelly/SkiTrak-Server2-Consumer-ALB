package consumer;

import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Logger;

/**
 * Entry point for the consumer application.
 * Connects to RabbitMQ, starts message consumers, and manages application shutdown.
 *
 * <pre>
 * java -jar consumer/target/ski-consumer-1.0-SNAPSHOT.jar [threads]
 * </pre>
 */
public class ConsumerApp {

    static {
        // Configure one-line log output before creating the first Logger.
        if (System.getProperty("java.util.logging.SimpleFormatter.format") == null) {
            System.setProperty("java.util.logging.SimpleFormatter.format",
                    "%1$tF %1$tT.%1$tL %4$s [%3$s] %5$s%6$s%n");
        }
    }

    private static final Logger LOG = Logger.getLogger(ConsumerApp.class.getName());
    private static final long STATS_INTERVAL_MS = 10_000;
    private static final long HEALTH_CHECK_INTERVAL_MS = 10_000;
    private static final int MAX_FAILED_HEALTH_CHECKS = 3;

    public static void main(String[] args) throws Exception {
        // Load consumer settings from environment variables, system properties, and command-line arguments.
        ConsumerConfig config = ConsumerConfig.fromEnvironment(args.length > 0 ? args[0] : null);
        LOG.info("starting consumer: " + config);

        // Establish the initial RabbitMQ connection, retrying until the broker is reachable.
        Connection connection = connect(config.connectionFactory(), config.reconnectBackoffMs);

        // Create the shared leaderboard aggregator and the pool responsible for consuming messages.
        LeaderboardAggregator aggregator =
                new LeaderboardAggregator(config.topN, config.dedupWindowPerResort);
        ConsumerPool pool = new ConsumerPool(connection, config, aggregator);

        // Start the configured number of consumer workers.
        pool.resize(config.initialWorkers);

        // Optionally adjust the worker count according to the current workload.
        if (config.autoscale) {
            pool.startAutoscaler();
        }

        // Periodically report processing statistics for monitoring and debugging.
        ScheduledExecutorService reporter = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "consumer-stats");
            t.setDaemon(true);
            return t;
        });

        reporter.scheduleAtFixedRate(() -> LOG.info(String.format(
                        "threads=%d backlog=%d swipesProcessed=%d duplicates=%d retries=%d deadLettered=%d ackFailures=%d broadcasts=%d",
                        pool.size(), pool.backlog(), aggregator.swipesProcessed.get(), pool.duplicates.get(),
                        pool.retries.get(), pool.deadLettered.get(), pool.ackFailures.get(), pool.broadcasts.get())),
                STATS_INTERVAL_MS, STATS_INTERVAL_MS, TimeUnit.MILLISECONDS);

        // Exit when workers stay unhealthy, so systemd (Restart=always) restarts the consumer.
        // Several consecutive failures are required to tolerate a channel that is still recovering.
        AtomicInteger failedChecks = new AtomicInteger();
        reporter.scheduleAtFixedRate(() -> {
            boolean healthy;
            try {
                healthy = pool.workersHealthy();
            } catch (RuntimeException e) {
                healthy = false;
            }

            if (healthy) {
                failedChecks.set(0);
            } else if (failedChecks.incrementAndGet() >= MAX_FAILED_HEALTH_CHECKS) {
                LOG.severe("consumer workers unhealthy for " + failedChecks.get() + " checks; exiting for restart");
                System.exit(1);
            } else {
                LOG.warning("health check failed (" + failedChecks.get() + "/" + MAX_FAILED_HEALTH_CHECKS + ")");
            }
        }, HEALTH_CHECK_INTERVAL_MS, HEALTH_CHECK_INTERVAL_MS, TimeUnit.MILLISECONDS);

        // Expose GET /health: UP only when connected to RabbitMQ and every worker is healthy.
        HealthServer health = new HealthServer(config.healthPort,
                () -> connection.isOpen() && pool.workersHealthy(),
                () -> {
                    Map<String, Object> stats = new LinkedHashMap<>();
                    stats.put("rabbitmqConnected", connection.isOpen());
                    stats.put("threads", pool.size());
                    stats.put("backlog", pool.backlog());
                    stats.put("swipesProcessed", aggregator.swipesProcessed.get());
                    stats.put("duplicates", pool.duplicates.get());
                    stats.put("retries", pool.retries.get());
                    stats.put("deadLettered", pool.deadLettered.get());
                    stats.put("ackFailures", pool.ackFailures.get());
                    stats.put("broadcasts", pool.broadcasts.get());
                    // Measured consumer lag (processing start - publish time) since startup.
                    Map<String, Object> lag = pool.lag.summary();
                    stats.put("lagSamples", lag.get("samples"));
                    stats.put("lagMissingHeader", pool.lag.missingHeader.get());
                    stats.put("lagNegative", pool.lag.negative.get());
                    stats.put("lagP50Ms", lag.get("p50Ms"));
                    stats.put("lagP99Ms", lag.get("p99Ms"));
                    stats.put("lagMaxMs", lag.get("maxMs"));
                    return stats;
                });
        // GET /lag?from=<epoch ms>&to=<epoch ms>: lag of messages published in that window.
        health.addEndpoint("/lag", q -> pool.lag.window(
                Long.parseLong(q.get("from")), Long.parseLong(q.get("to"))));
        health.start();
        LOG.info("health endpoint at http://0.0.0.0:" + health.port() + "/health");

        // Keep the main thread waiting until the shutdown hook finishes its cleanup.
        CountDownLatch stopped = new CountDownLatch(1);

        // Release resources when the JVM is shutting down, such as after Ctrl+C.
        // System.out avoids logging-handler conflicts while Java Logging is also shutting down.
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            System.out.println("shutting down consumer");

            // Stop the periodic statistics reporter and the health endpoint.
            reporter.shutdownNow();
            health.close();

            // Stop workers and close their channels; unacknowledged messages can be requeued.
            pool.close();

            // Close the RabbitMQ connection, ignoring errors if it is already closed.
            try {
                connection.close(2000);
            } catch (Exception ignored) {
                // The connection may already have been closed.
            }

            // Print final processing counters before the application exits.
            System.out.println("consumer stopped: swipesProcessed="
                    + aggregator.swipesProcessed + " duplicates=" + pool.duplicates);

            // Notify the waiting main thread that shutdown cleanup has finished.
            stopped.countDown();
        }, "consumer-shutdown"));

        stopped.await();
    }

    /**
     * Retries the initial RabbitMQ connection until the broker is reachable.
     * Subsequent connection drops are handled by the configured automatic recovery.
     */
    private static Connection connect(ConnectionFactory factory, long backoffMs)
            throws InterruptedException {
        while (true) {
            try {
                return factory.newConnection("skitrak-consumer");
            } catch (Exception e) {
                LOG.warning("RabbitMQ not reachable (" + e.getMessage()
                        + "); retrying in " + backoffMs + " ms");

                // Wait before retrying to avoid repeatedly connecting without a delay.
                Thread.sleep(backoffMs);
            }
        }
    }
}
