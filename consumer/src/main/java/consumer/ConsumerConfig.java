package consumer;

import com.rabbitmq.client.ConnectionFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.TreeSet;
import java.util.function.Function;

/**
 * Stores Consumer configuration.
 * Values are read from JVM system properties, then environment variables, then defaults.
 */
public class ConsumerConfig {
    final String host;
    final int port;
    final String username;
    final String password;
    final String virtualHost;
    final long reconnectBackoffMs;

    final List<Integer> resortIds;
    final int initialWorkers;
    final int minWorkers;
    final int maxWorkers;
    final int prefetch;
    final int ackBatch;

    final boolean autoscale;
    final long autoscaleIntervalMs;
    final int scaleUpBacklogPerWorker;
    final int scaleDownBacklog;

    final int topN;
    final int dedupWindowPerResort;
    final int maxAttempts;
    final long retryBackoffMs;

    final int healthPort;

    ConsumerConfig(Function<String, String> source) {
        // RabbitMQ connection settings.
        host = get(source, "RABBITMQ_HOST", "localhost");
        port = intValue(source, "RABBITMQ_PORT", 5672);
        username = get(source, "RABBITMQ_USERNAME", "guest");
        password = get(source, "RABBITMQ_PASSWORD", "guest");
        virtualHost = get(source, "RABBITMQ_VHOST", "/");
        reconnectBackoffMs = longValue(source, "RABBITMQ_RECONNECT_BACKOFF_MS", 2000);

        resortIds = parseResortIds(get(source, "CONSUMER_RESORT_IDS", "1-10"));

        // Workers beyond the number of resort queues subscribe as single-active-consumer standbys.
        maxWorkers = Math.max(1,
                intValue(source, "CONSUMER_MAX_THREADS", resortIds.size()));
        minWorkers = clamp(
                intValue(source, "CONSUMER_MIN_THREADS", 1),
                1, maxWorkers);
        initialWorkers = clamp(
                intValue(source, "CONSUMER_THREADS", Math.min(5, maxWorkers)),
                minWorkers, maxWorkers);
        prefetch = intValue(source, "CONSUMER_PREFETCH", 100);
        // Number of processed deliveries acknowledged together; 1 acks each one.
        ackBatch = Math.max(1, intValue(source, "CONSUMER_ACK_BATCH", 1));

        // Autoscaling thresholds and polling interval.
        autoscale = Boolean.parseBoolean(get(source, "CONSUMER_AUTOSCALE", "true"));
        autoscaleIntervalMs = longValue(source, "CONSUMER_AUTOSCALE_INTERVAL_MS", 5000);
        scaleUpBacklogPerWorker = intValue(
                source, "CONSUMER_SCALE_UP_BACKLOG_PER_THREAD", 2000);
        scaleDownBacklog = intValue(source, "CONSUMER_SCALE_DOWN_BACKLOG", 100);

        // Leaderboard size, duplicate detection, and retry settings.
        topN = intValue(source, "LEADERBOARD_TOP_N", 20);
        dedupWindowPerResort = intValue(source, "CONSUMER_DEDUP_WINDOW", 200_000);
        maxAttempts = Math.max(1, intValue(source, "CONSUMER_MAX_ATTEMPTS", 3));
        retryBackoffMs = longValue(source, "CONSUMER_RETRY_BACKOFF_MS", 100);

        healthPort = intValue(source, "CONSUMER_HEALTH_PORT", 8081);
    }

    /**
     * Loads configuration with this priority:
     * CLI thread argument, JVM system property, environment variable, default.
     */
    public static ConsumerConfig fromEnvironment(String threadsArg) {
        Map<String, String> env = System.getenv();

        return new ConsumerConfig(key -> {
            if (threadsArg != null && key.equals("CONSUMER_THREADS")) {
                return threadsArg;
            }

            String value = System.getProperty(key);
            return value != null ? value : env.get(key);
        });
    }

    /**
     * Creates the RabbitMQ connection factory and enables automatic
     * recovery of connections, channels, topology, and consumers.
     */
    public ConnectionFactory connectionFactory() {
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost(host);
        factory.setPort(port);
        factory.setUsername(username);
        factory.setPassword(password);
        factory.setVirtualHost(virtualHost);
        factory.setConnectionTimeout(5000);
        factory.setRequestedHeartbeat(30);
        factory.setAutomaticRecoveryEnabled(true);
        factory.setTopologyRecoveryEnabled(true);
        factory.setNetworkRecoveryInterval(reconnectBackoffMs);
        return factory;
    }

    /** Parses ranges such as "1-10" and lists such as "1,3,5-7". */
    static List<Integer> parseResortIds(String spec) {
        TreeSet<Integer> ids = new TreeSet<>();

        for (String part : spec.split(",")) {
            String p = part.trim();
            if (p.isEmpty()) {
                continue;
            }

            int dash = p.indexOf('-');
            if (dash > 0) {
                int from = Integer.parseInt(p.substring(0, dash).trim());
                int to = Integer.parseInt(p.substring(dash + 1).trim());

                for (int id = from; id <= to; id++) {
                    ids.add(id);
                }
            } else {
                ids.add(Integer.parseInt(p));
            }
        }

        if (ids.isEmpty() || ids.first() < 1) {
            throw new IllegalArgumentException(
                    "CONSUMER_RESORT_IDS must list positive resort IDs: " + spec);
        }

        // Return sorted, unique IDs that cannot be modified by callers.
        return Collections.unmodifiableList(new ArrayList<>(ids));
    }

    private static int clamp(int value, int min, int max) {
        return Math.max(min, Math.min(max, value));
    }

    private static String get(
            Function<String, String> source, String key, String defaultValue) {
        String value = source.apply(key);
        return value == null || value.isBlank() ? defaultValue : value.trim();
    }

    private static int intValue(
            Function<String, String> source, String key, int defaultValue) {
        return Integer.parseInt(get(source, key, Integer.toString(defaultValue)));
    }

    private static long longValue(
            Function<String, String> source, String key, long defaultValue) {
        return Long.parseLong(get(source, key, Long.toString(defaultValue)));
    }

    @Override
    public String toString() {
        return "rabbitmq=" + host + ":" + port + ", resorts=" + resortIds
                + ", threads=" + initialWorkers + " (min " + minWorkers + ", max " + maxWorkers
                + ", autoscale " + autoscale + "), prefetch=" + prefetch + ", ackBatch=" + ackBatch + ", topN=" + topN
                + ", maxAttempts=" + maxAttempts;
    }
}