package consumer;

import com.rabbitmq.client.BuiltinExchangeType;
import com.rabbitmq.client.Channel;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Defines and initializes the RabbitMQ topology used by the consumer.
 *
 * The declarations are idempotent when existing resources have matching
 * configurations. Missing resources are created automatically.
 *
 * Message flow:
 *
 * skitrak.exchange (topic)
 *     -- resort.{id} --> skitrak.resort.{id}
 *                              |
 *                              | rejected after retries
 *                              v
 *                      skitrak.dlx (fanout)
 *                              |
 *                              v
 *                         skitrak.dlq
 *
 * Consumer
 *     --> skitrak.leaderboard (fanout)
 *     --> temporary queue created by each WebSocket server
 */
public final class Topology {

    public static final String SWIPE_EXCHANGE = "skitrak.exchange";
    public static final String RESORT_QUEUE_PREFIX = "skitrak.resort.";
    public static final String DEAD_LETTER_EXCHANGE = "skitrak.dlx";
    public static final String DEAD_LETTER_QUEUE = "skitrak.dlq";
    public static final String LEADERBOARD_EXCHANGE = "skitrak.leaderboard";

    private Topology() {}

    /**
     * Returns the queue name for a resort.
     */
    public static String resortQueue(int resortID) {
        return RESORT_QUEUE_PREFIX + resortID;
    }

    /**
     * Returns the routing key used to route swipe events to a resort queue.
     */
    public static String routingKey(int resortID) {
        return "resort." + resortID;
    }

    /**
     * Declares the exchanges, queues, and bindings required by the consumer.
     *
     * Existing resources must have compatible declaration parameters.
     * RabbitMQ reuses matching resources but rejects declarations that
     * conflict with the configuration of existing resources.
     *
     * @param channel   RabbitMQ channel used to declare the topology
     * @param resortIds resort IDs whose queues should be configured
     * @throws IOException if a declaration or binding operation fails
     */
    public static void declare(Channel channel, List<Integer> resortIds) throws IOException {
        // Exchange used by the server to publish swipe events.
        channel.exchangeDeclare(SWIPE_EXCHANGE, BuiltinExchangeType.TOPIC, true);

        // Dead-letter exchange and queue for messages rejected without requeueing.
        channel.exchangeDeclare(DEAD_LETTER_EXCHANGE, BuiltinExchangeType.FANOUT, true);
        channel.queueDeclare(DEAD_LETTER_QUEUE, true, false, false, null);
        channel.queueBind(DEAD_LETTER_QUEUE, DEAD_LETTER_EXCHANGE, "");

        // Fanout exchange for broadcasting leaderboard updates to WebSocket servers.
        // Each WebSocket server creates and binds its own temporary queue.
        channel.exchangeDeclare(LEADERBOARD_EXCHANGE, BuiltinExchangeType.FANOUT, true);

        Map<String, Object> args = new HashMap<>();

        // Allow only one active consumer per resort queue, preserving per-resort order.
        // Additional subscribers wait as standbys and take over if the active one leaves.
        args.put("x-single-active-consumer", true);

    // Route rejected messages to the dead-letter exchange.
            args.put("x-dead-letter-exchange", DEAD_LETTER_EXCHANGE);

    // Messages expire after 60 seconds in the queue.
            args.put("x-message-ttl", 60_000);

    // Limit each resort queue to 10,000 messages.
            args.put("x-max-length", 10_000);



        for (int resortID : resortIds) {
            String queue = resortQueue(resortID);

            // Create a durable queue for each resort with the required arguments.
            channel.queueDeclare(queue, true, false, false, args);

            // Route swipe events to the queue matching this resort ID.
            channel.queueBind(queue, SWIPE_EXCHANGE, routingKey(resortID));
        }
    }
}
