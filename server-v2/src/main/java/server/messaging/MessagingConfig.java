package server.messaging;

import com.rabbitmq.client.ConnectionFactory;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Map;
import java.util.function.Function;

/**
 * RabbitMQ and server settings. Each value is read from a JVM system property first
 * (e.g. {@code -DRABBITMQ_HOST=10.0.0.5} in Tomcat's {@code setenv.sh}), then from an
 * environment variable of the same name, then the default.
 */
public class MessagingConfig {
    final String host;
    final int port;
    final String username;
    final String password;
    final String virtualHost;
    final int channelPoolSize;
    final long borrowTimeoutMs;
    final long reconnectBackoffMs;
    final boolean publisherConfirms;
    final long confirmTimeoutMs;
    final String serverId;

    MessagingConfig(Function<String, String> source) {
        host = get(source, "RABBITMQ_HOST", "localhost");
        port = Integer.parseInt(get(source, "RABBITMQ_PORT", "5672"));
        username = get(source, "RABBITMQ_USERNAME", "guest");
        password = get(source, "RABBITMQ_PASSWORD", "guest");
        virtualHost = get(source, "RABBITMQ_VHOST", "/");
        channelPoolSize = Integer.parseInt(get(source, "RABBITMQ_CHANNEL_POOL_SIZE", "64"));
        borrowTimeoutMs = Long.parseLong(get(source, "RABBITMQ_BORROW_TIMEOUT_MS", "2000"));
        reconnectBackoffMs = Long.parseLong(get(source, "RABBITMQ_RECONNECT_BACKOFF_MS", "1000"));
        publisherConfirms = Boolean.parseBoolean(get(source, "RABBITMQ_PUBLISHER_CONFIRMS", "true"));
        confirmTimeoutMs = Long.parseLong(get(source, "RABBITMQ_CONFIRM_TIMEOUT_MS", "5000"));
        serverId = get(source, "SERVER_ID", defaultServerId());
    }

    // Loads configuration from JVM system properties first, then environment variables.
    public static MessagingConfig fromEnvironment() {
        Map<String, String> env = System.getenv();
        return new MessagingConfig(key -> {
            String value = System.getProperty(key);
            return value != null ? value : env.get(key);
        });
    }

    // Creates the RabbitMQ connection factory using the configured connection settings.
    public ConnectionFactory connectionFactory() {
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost(host);
        factory.setPort(port);
        factory.setUsername(username);
        factory.setPassword(password);
        factory.setVirtualHost(virtualHost);
        factory.setConnectionTimeout(5000);
        factory.setRequestedHeartbeat(30); // detects silently dropped TCP connections
        return factory;
    }

    public String getServerId() {
        return serverId;
    }

    // Returns the configured value, or the default if the value is missing or blank.
    private static String get(Function<String, String> source, String key, String defaultValue) {
        String value = source.apply(key);
        return value == null || value.isBlank() ? defaultValue : value.trim();
    }

    // Uses the local hostname as the server ID, with a fallback if hostname lookup fails.
    private static String defaultServerId() {
        try {
            return InetAddress.getLocalHost().getHostName();
        } catch (UnknownHostException e) {
            return "skitrak-server";
        }
    }
}