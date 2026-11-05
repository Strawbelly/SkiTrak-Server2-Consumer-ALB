package server.messaging;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Thread-safe pool of RabbitMQ channels sharing one connection.
 * Each borrowed channel must be used by one thread at a time and returned after use.
 * Reconnects and rebuilds the pool when necessary.
 */
public class ChannelPool implements AutoCloseable {
    private static final Logger LOG = Logger.getLogger(ChannelPool.class.getName());

    /** Initializes each new channel before it enters the pool. */
    @FunctionalInterface
    public interface ChannelInitializer {
        void initialize(Channel channel) throws IOException;
    }

    private final BlockingQueue<Channel> pool;
    private volatile Connection connection;
    private final ConnectionFactory factory;
    private final int poolSize;
    private final long borrowTimeoutMs;
    private final long reconnectBackoffNanos;
    private final ChannelInitializer initializer;

    // Serializes connection and pool rebuild operations.
    private final Object reconnectLock = new Object();
    private volatile boolean closed;
    private long nextReconnectAllowedAt;   // System.nanoTime(); guarded by reconnectLock
    private boolean reconnectFailedBefore; // guarded by reconnectLock

    public ChannelPool(ConnectionFactory factory, int poolSize, long borrowTimeoutMs,
                       long reconnectBackoffMs, ChannelInitializer initializer) {
        if (poolSize < 1) {
            throw new IllegalArgumentException("poolSize must be >= 1");
        }
        this.factory = factory;
        this.poolSize = poolSize;
        this.borrowTimeoutMs = borrowTimeoutMs;
        this.reconnectBackoffNanos = TimeUnit.MILLISECONDS.toNanos(reconnectBackoffMs);
        this.initializer = initializer == null ? ch -> { } : initializer;
        this.pool = new LinkedBlockingQueue<>(poolSize);

        factory.setAutomaticRecoveryEnabled(false);
        factory.setTopologyRecoveryEnabled(false);
    }

    /** Opens the shared connection and initializes the channel pool. */
    public void init() throws IOException {
        synchronized (reconnectLock) {
            checkNotClosed();
            connect();
        }
    }

    /**
     * Borrows a channel, waiting up to the configured timeout.
     * Reconnects if the shared connection is unavailable.
     */
    public Channel borrowChannel() throws IOException, InterruptedException {
        long deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(borrowTimeoutMs);
        while (true) {
            checkNotClosed();
            ensureConnected();

            long remaining = deadline - System.nanoTime();
            Channel channel = pool.poll(Math.max(remaining, 0), TimeUnit.NANOSECONDS);
            if (channel == null) {
                throw new IOException("timed out after " + borrowTimeoutMs + " ms waiting for a channel");
            }
            if (channel.isOpen()) {
                return channel;
            }

            // Replace a failed channel if its shared connection is still healthy.
            Connection current = connection;
            if (channel.getConnection() == current && current != null && current.isOpen()) {
                // Only this channel failed: hand the caller a replacement (pool size is unchanged).
                return newChannel(current);
            }
            // Otherwise, loop to reconnect and rebuild the pool.
        }
    }

    /**
     * Returns a channel to the pool, replacing it if necessary
     * and discarding channels from an old connection.
     */
    public void returnChannel(Channel channel) {
        if (channel == null) {
            return;
        }
        Connection current = connection;
        if (closed || channel.getConnection() != current) {
            closeQuietly(channel);
            return;
        }
        if (channel.isOpen()) {
            if (!pool.offer(channel)) {
                closeQuietly(channel); // should not happen: more channels than the pool holds
            }
            return;
        }

        // Replace a closed channel if the current connection is still healthy.
        if (current != null && current.isOpen()) {
            try {
                if (!pool.offer(newChannel(current))) {
                    LOG.warning("channel pool full; dropping replacement channel");
                }
            } catch (IOException e) {
                LOG.log(Level.WARNING, "could not replace closed channel", e);
            }
        }
    }

    /** Closes all pooled channels and the connection. */
    @Override
    public void close() {
        closed = true;
        synchronized (reconnectLock) {
            discardAll();
        }
    }

    public boolean isConnected() {
        Connection c = connection;
        return c != null && c.isOpen();
    }

    public int getPoolSize() {
        return poolSize;
    }

    /** Returns the number of idle channels available to borrow. */
    public int availableChannels() {
        return pool.size();
    }

    // Visible to tests that simulate a dropped connection.
    Connection currentConnection() {
        return connection;
    }

    /**
     * Reconnects when necessary. Failed attempts are rate-limited
     * to avoid repeatedly blocking requests while RabbitMQ is unavailable.
     */
    private void ensureConnected() throws IOException {
        if (isConnected()) {
            return;
        }
        synchronized (reconnectLock) {
            checkNotClosed();
            if (isConnected()) {
                return; // another thread already reconnected
            }
            long now = System.nanoTime();
            if (reconnectFailedBefore && now - nextReconnectAllowedAt < 0) {
                throw new IOException("RabbitMQ connection is down; waiting before next reconnect attempt");
            }
            LOG.warning("RabbitMQ connection is not open; reconnecting and rebuilding channel pool");
            try {
                connect();
                reconnectFailedBefore = false;
                LOG.info("RabbitMQ reconnected; channel pool rebuilt with " + poolSize + " channels");
            } catch (IOException e) {
                reconnectFailedBefore = true;
                nextReconnectAllowedAt = now + reconnectBackoffNanos;
                throw e;
            }
        }
    }

    // Caller holds reconnectLock.
    private void connect() throws IOException {
        discardAll();

        Connection conn;
        try {
            conn = factory.newConnection("skitrak-server");
        } catch (TimeoutException e) {
            throw new IOException("timed out connecting to RabbitMQ", e);
        }
        conn.addShutdownListener(cause -> {
            if (!cause.isInitiatedByApplication()) {
                LOG.warning("RabbitMQ connection lost: " + cause.getMessage());
            }
        });

        List<Channel> fresh = new ArrayList<>(poolSize);
        try {
            for (int i = 0; i < poolSize; i++) {
                fresh.add(newChannel(conn));
            }
        } catch (IOException | RuntimeException e) {
            closeQuietly(conn);
            throw e;
        }
        // Publish the connection before making its channels available.
        connection = conn;
        pool.addAll(fresh);
    }

    private Channel newChannel(Connection conn) throws IOException {
        Channel channel = conn.createChannel();
        if (channel == null) {
            throw new IOException("broker refused a new channel (channel_max reached)");
        }
        try {
            initializer.initialize(channel);
        } catch (IOException | RuntimeException e) {
            closeQuietly(channel);
            throw e;
        }
        return channel;
    }

    // Caller holds reconnectLock.
    private void discardAll() {
        Channel ch;
        while ((ch = pool.poll()) != null) {
            closeQuietly(ch);
        }
        Connection old = connection;
        connection = null;
        if (old != null) {
            closeQuietly(old);
        }
    }

    private void checkNotClosed() {
        if (closed) {
            throw new IllegalStateException("channel pool is closed");
        }
    }

    static void closeQuietly(Channel channel) {
        try {
            if (channel.isOpen()) {
                channel.close();
            }
        } catch (IOException | TimeoutException | RuntimeException ignored) {
            // Ignore cleanup errors.
        }
    }

    private static void closeQuietly(Connection conn) {
        try {
            if (conn.isOpen()) {
                conn.close(1000);
            }
        } catch (IOException | RuntimeException ignored) {
            // Ignore cleanup errors.
        }
    }
}
