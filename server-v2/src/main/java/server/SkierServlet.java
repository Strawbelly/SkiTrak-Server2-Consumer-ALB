package server;

import com.google.gson.Gson;
import com.google.gson.JsonSyntaxException;
import server.messaging.MessagingContextListener;
import server.messaging.SwipeEventPublisher;
import server.model.LiftRide;
import server.model.ResponseMsg;
import server.model.SwipeEvent;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.BufferedReader;
import java.io.IOException;
import java.util.OptionalInt;
import java.util.UUID;
import java.util.logging.Logger;

public class SkierServlet extends HttpServlet {
    private static final Logger LOG = Logger.getLogger(SkierServlet.class.getName());

    private Gson gson = new Gson();
    private SwipeEventPublisher publisher;
    private LiftVerticalMap liftVerticals;
    private String serverId;

    public SkierServlet() {}

    // Constructor for tests to inject dependencies without requiring a ServletContext.
    SkierServlet(SwipeEventPublisher publisher, LiftVerticalMap liftVerticals, String serverId) {
        this.publisher = publisher;
        this.liftVerticals = liftVerticals;
        this.serverId = serverId;
    }

    @Override
    public void init() throws ServletException {
        // Load the lift-to-vertical mapping if it was not injected by a test.
        if (liftVerticals == null) {
            liftVerticals = LiftVerticalMap.fromClasspath();
        }

        // Retrieve the shared publisher and server ID initialized by the context listener.
        if (publisher == null) {
            publisher = (SwipeEventPublisher) getServletContext()
                    .getAttribute(MessagingContextListener.PUBLISHER_ATTR);
            serverId = (String) getServletContext()
                    .getAttribute(MessagingContextListener.SERVER_ID_ATTR);
        }

        // Fail initialization if the publisher is unavailable.
        if (publisher == null) {
            throw new ServletException("SwipeEventPublisher not initialized; is MessagingContextListener registered?");
        }
    }

    @Override
    protected void doPost(HttpServletRequest req, HttpServletResponse res)
            throws ServletException, IOException {

        res.setContentType("application/json");
        res.setCharacterEncoding("UTF-8");

        String urlPath = req.getPathInfo();

        // Validate that the request contains a path matching the expected endpoint.
        if (urlPath == null || urlPath.isEmpty()) {
            res.setStatus(HttpServletResponse.SC_NOT_FOUND);
            res.getWriter().write(gson.toJson(new ResponseMsg("missing parameters")));
            return;
        }

        String[] urlParts = urlPath.split("/");

        if (!isUrlValid(urlParts)) {
            res.setStatus(HttpServletResponse.SC_BAD_REQUEST);
            res.getWriter().write(gson.toJson(new ResponseMsg("invalid url")));
            return;
        }

        // Parse and validate the swipe request body.
        try {
            StringBuilder jsonBody = new StringBuilder();
            BufferedReader reader = req.getReader();
            String line;
            while ((line = reader.readLine()) != null) {
                jsonBody.append(line);
            }

            LiftRide liftRide = gson.fromJson(jsonBody.toString(), LiftRide.class);

            if (liftRide == null || !isLiftRideValid(liftRide)) {
                res.setStatus(HttpServletResponse.SC_BAD_REQUEST);
                res.getWriter().write(gson.toJson(new ResponseMsg("invalid JSON body")));
                return;
            }

            // Look up the vertical distance associated with the submitted lift ID.
            OptionalInt vertical = liftVerticals.getVertical(liftRide.getLiftID());
            if (vertical.isEmpty()) {
                res.setStatus(HttpServletResponse.SC_BAD_REQUEST);
                res.getWriter().write(gson.toJson(new ResponseMsg("invalid JSON body")));
                return;
            }

            // Build the event that will be published to RabbitMQ.
            SwipeEvent event = new SwipeEvent(
                    UUID.randomUUID().toString(),
                    Integer.parseInt(urlParts[1]),   // resortID
                    Integer.parseInt(urlParts[5]),   // dayID
                    Integer.parseInt(urlParts[7]),   // skierID
                    liftRide.getLiftID(),
                    vertical.getAsInt(),
                    liftRide.getTime(),
                    System.currentTimeMillis(),
                    serverId);

            // Publish the event; return 503 if publishing fails so the client can retry.
            try {
                publisher.publish(event);
            } catch (IOException | RuntimeException e) {
                LOG.warning("failed to publish swipe event " + event.getEventId() + ": " + e);
                writeUnavailable(res);
                return;
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                writeUnavailable(res);
                return;
            }

            res.setStatus(HttpServletResponse.SC_CREATED);

        } catch (JsonSyntaxException e) {
            // Return 400 when the request body is not valid JSON.
            res.setStatus(HttpServletResponse.SC_BAD_REQUEST);
            res.getWriter().write(gson.toJson(new ResponseMsg("invalid JSON format")));
        }
    }

    // Return 503 when the event cannot be published to RabbitMQ.
    private void writeUnavailable(HttpServletResponse res) throws IOException {
        res.setStatus(HttpServletResponse.SC_SERVICE_UNAVAILABLE);
        res.getWriter().write(gson.toJson(new ResponseMsg("event queue unavailable")));
    }

    // Validate the endpoint path structure and ensure all IDs are positive integers.
    private boolean isUrlValid(String[] urlParts) {
        if (urlParts.length != 8) {
            return false;
        }
        try {
            if (!urlParts[2].equals("seasons") ||
                    !urlParts[4].equals("days") ||
                    !urlParts[6].equals("skiers")) {
                return false;
            }
            return Integer.parseInt(urlParts[1]) > 0 &&
                    Integer.parseInt(urlParts[3]) > 0 &&
                    Integer.parseInt(urlParts[5]) > 0 &&
                    Integer.parseInt(urlParts[7]) > 0;
        } catch (NumberFormatException e) {
            return false;
        }
    }

    // Validate the lift ride's time and lift ID against the accepted ranges.
    private boolean isLiftRideValid(LiftRide liftRide) {
        return liftRide.getTime() >= 1 &&
                liftRide.getTime() <= 360 &&
                liftRide.getLiftID() >= 1 &&
                liftRide.getLiftID() <= 40;
    }
}
