package consumer;

import com.google.gson.Gson;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.LinkedHashMap;
import java.util.HashMap;
import java.util.Map;
import java.util.function.BooleanSupplier;
import java.util.function.Function;
import java.util.function.Supplier;

/**
 * Provides an HTTP endpoint for checking the consumer's health.
 *
 * GET /health returns 200 when healthy or 503 when unhealthy.
 */
final class HealthServer implements AutoCloseable {
    private static final Gson GSON = new Gson();

    private final HttpServer server;
    private final BooleanSupplier healthy;
    private final Supplier<Map<String, Object>> details;

    /**
     * Creates the health endpoint.
     * Port 0 lets the operating system choose an available port for tests.
     */
    HealthServer(int port, BooleanSupplier healthy,
                 Supplier<Map<String, Object>> details) throws IOException {
        this.healthy = healthy;
        this.details = details;

        server = HttpServer.create(new InetSocketAddress(port), 0);
        server.createContext("/health", this::handle);

        // Use the JDK's default executor to handle HTTP requests.
        server.setExecutor(null);
    }

    /**
     * Adds a read-only GET endpoint that returns JSON computed from the query
     * parameters, for example /lag?from=...&to=.... Call before start().
     */
    void addEndpoint(String path, Function<Map<String, String>, Map<String, Object>> handler) {
        server.createContext(path, exchange -> {
            try {
                if (!path.equals(exchange.getRequestURI().getPath())) {
                    send(exchange, 404, "{\"error\":\"not found\"}");
                } else if (!"GET".equals(exchange.getRequestMethod())) {
                    send(exchange, 405, "{\"error\":\"method not allowed\"}");
                } else {
                    try {
                        send(exchange, 200, GSON.toJson(handler.apply(query(exchange))));
                    } catch (RuntimeException e) {
                        send(exchange, 400, GSON.toJson(Map.of("error", e.toString())));
                    }
                }
            } finally {
                exchange.close();
            }
        });
    }

    private static Map<String, String> query(HttpExchange exchange) {
        Map<String, String> params = new HashMap<>();
        String raw = exchange.getRequestURI().getQuery();
        if (raw != null) {
            for (String pair : raw.split("&")) {
                int eq = pair.indexOf('=');
                if (eq > 0) {
                    params.put(pair.substring(0, eq), pair.substring(eq + 1));
                }
            }
        }
        return params;
    }

    void start() {
        server.start();
    }

    int port() {
        return server.getAddress().getPort();
    }

    @Override
    public void close() {
        // Stop the HTTP server during application shutdown.
        server.stop(0);
    }

    private void handle(HttpExchange exchange) throws IOException {
        try {
            // Reject requests to paths other than /health.
            if (!"/health".equals(exchange.getRequestURI().getPath())) {
                send(exchange, 404, "{\"error\":\"not found\"}");
                return;
            }

            // Only GET requests are supported.
            if (!"GET".equals(exchange.getRequestMethod())) {
                send(exchange, 405, "{\"error\":\"method not allowed\"}");
                return;
            }

            boolean up;
            Map<String, Object> body = new LinkedHashMap<>();

            try {
                // Check health and include the current consumer statistics.
                up = healthy.getAsBoolean();
                body.put("status", up ? "UP" : "DOWN");
                body.putAll(details.get());
            } catch (RuntimeException e) {
                // Treat a failed health check as unhealthy.
                up = false;
                body.put("status", "DOWN");
                body.put("error", e.toString());
            }

            send(exchange, up ? 200 : 503, GSON.toJson(body));
        } finally {
            // Release the resources associated with this HTTP request.
            exchange.close();
        }
    }

    private static void send(HttpExchange exchange, int status, String json)
            throws IOException {
        byte[] bytes = json.getBytes(StandardCharsets.UTF_8);

        exchange.getResponseHeaders().set("Content-Type", "application/json");
        exchange.sendResponseHeaders(status, bytes.length);

        try (OutputStream out = exchange.getResponseBody()) {
            out.write(bytes);
        }
    }
}
