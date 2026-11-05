package server.messaging;

import javax.servlet.ServletContext;
import javax.servlet.ServletContextEvent;
import javax.servlet.ServletContextListener;
import java.io.IOException;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Creates the shared {@link ChannelPool} and {@link SwipeEventPublisher} when the webapp
 * starts and closes them when it stops. SkierServlet reads them from context attributes.
 */
public class MessagingContextListener implements ServletContextListener {
    // Keys used to store shared messaging resources in the ServletContext.
    public static final String PUBLISHER_ATTR = "skitrak.swipeEventPublisher";
    public static final String SERVER_ID_ATTR = "skitrak.serverId";

    private static final Logger LOG = Logger.getLogger(MessagingContextListener.class.getName());

    private ChannelPool pool;
    private LeaderboardRelay relay;

    @Override
    public void contextInitialized(ServletContextEvent sce) {
        // Load RabbitMQ connection and publishing settings.
        MessagingConfig config = MessagingConfig.fromEnvironment();

        // Create the shared channel pool and configure channel initialization.
        pool = new ChannelPool(config.connectionFactory(), config.channelPoolSize,
                config.borrowTimeoutMs, config.reconnectBackoffMs,
                SwipeEventPublisher.channelInitializer(config.publisherConfirms));
        try {
            pool.init();
            LOG.info("Connected to RabbitMQ at " + config.host + ":" + config.port
                    + " with " + config.channelPoolSize + " pooled channels");
        } catch (IOException e) {
            // Keep the webapp up; the pool reconnects on the first publish.
            LOG.log(Level.WARNING, "RabbitMQ unavailable at startup (" + config.host + ":"
                    + config.port + "); will retry on first publish", e);
        }

        // Make the publisher and server ID available to servlets through the shared context.
        ServletContext ctx = sce.getServletContext();
        ctx.setAttribute(PUBLISHER_ATTR,
                new SwipeEventPublisher(pool, config.publisherConfirms, config.confirmTimeoutMs));
        ctx.setAttribute(SERVER_ID_ATTR, config.getServerId());

        // Subscribe this server to the leaderboard fanout exchange and relay updates to its WebSocket sessions.
        relay = new LeaderboardRelay(config.connectionFactory(), config.getServerId(), config.reconnectBackoffMs);
        relay.start();
    }

    @Override
    public void contextDestroyed(ServletContextEvent sce) {
        // Release RabbitMQ channels and connections when the webapp shuts down.
        if (relay != null) {
            relay.close();
        }
        if (pool != null) {
            pool.close();
        }
    }
}