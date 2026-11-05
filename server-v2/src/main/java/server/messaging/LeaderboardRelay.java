package server.messaging;

import com.google.gson.Gson;
import com.google.gson.JsonParseException;
import com.rabbitmq.client.BuiltinExchangeType;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import server.model.Leaderboard;
import server.websocket.LeaderboardEndpoint;

import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Logger;

/**
 * Subscribes the WebSocket server to the skitrak.leaderboard fanout exchange
 * and forwards leaderboard snapshots to local WebSocket clients.
 *
 * Each server instance creates its own exclusive, auto-delete queue so that
 * every server receives every leaderboard update. Since updates contain full
 * snapshots, the queue is bounded and uses auto-acknowledgment; older queued
 * updates may be dropped when the queue reaches its limit.
 */
public class LeaderboardRelay implements AutoCloseable {

    public static final String LEADERBOARD_EXCHANGE = "skitrak.leaderboard";
    static final int MAX_QUEUED_UPDATES = 1000;

    private static final Logger LOG =
            Logger.getLogger(LeaderboardRelay.class.getName());
    private static final Gson GSON = new Gson();

    private final ConnectionFactory factory;
    private final String serverId;
    private final long retryMs;

    // Volatile ensures connection state and shutdown requests are visible across threads.
    private volatile Connection connection;
    private volatile boolean closed;

    public LeaderboardRelay(ConnectionFactory factory, String serverId, long retryMs) {
        this.factory = factory;
        this.serverId = serverId;
        this.retryMs = Math.max(100, retryMs);

        // Allow RabbitMQ to restore the connection and its topology after a disconnection.
        factory.setAutomaticRecoveryEnabled(true);
        factory.setTopologyRecoveryEnabled(true);
        factory.setNetworkRecoveryInterval(this.retryMs);
    }

    /**
     * Starts a background thread that retries the initial connection until
     * RabbitMQ becomes reachable or this relay is closed.
     */
    public void start() {
        Thread t = new Thread(() -> {
            boolean warned = false;

            while (!closed && connection == null) {
                try {
                    connect();
                } catch (Exception e) {
                    // Avoid repeatedly logging the same initial connection failure.
                    if (!warned) {
                        LOG.warning("leaderboard relay cannot reach RabbitMQ ("
                                + e + "); retrying every " + retryMs + " ms");
                        warned = true;
                    }

                    try {
                        Thread.sleep(retryMs);
                    } catch (InterruptedException ie) {
                        Thread.currentThread().interrupt();
                        return;
                    }
                }
            }
        }, "leaderboard-relay-connect");

        // This thread should not prevent the JVM from exiting.
        t.setDaemon(true);
        t.start();
    }

    /**
     * Stops the relay and closes its RabbitMQ connection.
     */
    @Override
    public void close() {
        closed = true;

        Connection c = connection;
        if (c != null) {
            try {
                c.close(1000);
            } catch (Exception ignored) {
                // The connection may already be closed.
            }
        }
    }

    /**
     * Establishes the RabbitMQ connection, declares the fanout exchange,
     * creates a bounded queue, binds it to the exchange, and starts consuming.
     */
    private void connect() throws Exception {
        Connection conn = factory.newConnection(
                "skitrak-leaderboard-relay-" + serverId);

        try {
            Channel ch = conn.createChannel();

            // All queues bound to this fanout exchange receive each published update.
            ch.exchangeDeclare(
                    LEADERBOARD_EXCHANGE, BuiltinExchangeType.FANOUT, true);

            Map<String, Object> args = new HashMap<>();
            args.put("x-max-length", MAX_QUEUED_UPDATES);
            args.put("x-overflow", "drop-head");

            // Let RabbitMQ generate a unique queue name for this server instance.
            // Exclusive queues belong to this connection and are deleted when it closes.
            String queue = ch.queueDeclare(
                    "", false, true, true, args).getQueue();

            ch.queueBind(queue, LEADERBOARD_EXCHANGE, "");

            // Auto-ack is appropriate here because each message is a full snapshot.
            // If the queue overflows, older snapshots may be discarded.
            ch.basicConsume(queue, true,
                    (tag, delivery) -> forward(delivery.getBody()),
                    tag -> { });

            LOG.info("subscribed to " + LEADERBOARD_EXCHANGE
                    + " via queue " + queue);

        } catch (Exception e) {
            conn.abort();
            throw e;
        }

        // Avoid keeping the connection if shutdown happened during initialization.
        if (closed) {
            conn.abort();
            return;
        }

        connection = conn;
    }

    /**
     * Parses a RabbitMQ message and forwards the leaderboard snapshot
     * to the local WebSocket endpoint.
     *
     * This callback runs on the RabbitMQ connection's dispatch thread.
     */
    static void forward(byte[] body) {
        Update u;

        try {
            u = GSON.fromJson(
                    new String(body, StandardCharsets.UTF_8), Update.class);
        } catch (JsonParseException e) {
            // Ignore malformed messages instead of interrupting message processing.
            LOG.warning("ignoring malformed leaderboard update: "
                    + e.getMessage());
            return;
        }

        if (u != null && u.top != null) {
            // The endpoint sends the snapshot to WebSocket clients subscribed
            // to the corresponding resort and updates its cached leaderboard.
            LeaderboardEndpoint.broadcast(
                    u.resortID, u.dayID, u.version, u.generatedAt, u.top);
        }
    }

    /**
     * Matches the JSON message format published by
     * consumer.model.LeaderboardUpdate.
     */
    private static class Update {
        int resortID;
        int dayID;
        long version;
        long generatedAt;
        List<Leaderboard.Record> top;
    }
}
