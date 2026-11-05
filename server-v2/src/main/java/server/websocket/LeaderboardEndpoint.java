package server.websocket;

import com.google.gson.Gson;
import server.model.Leaderboard;

import javax.websocket.OnClose;
import javax.websocket.OnError;
import javax.websocket.OnOpen;
import javax.websocket.Session;
import javax.websocket.server.PathParam;
import javax.websocket.server.ServerEndpoint;
import java.io.IOException;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Pushes real leaderboards for one resort. Updates arrive from the consumer through the
 * {@code skitrak.leaderboard} fanout exchange ({@link server.messaging.LeaderboardRelay}) and
 * are sent only when the standings change. A new session first gets the latest standings
 * this server has seen for each day of the resort.
 */
@ServerEndpoint("/leaderboard/{resortID}")
public class LeaderboardEndpoint {
    private static final DateTimeFormatter ISO_MILLIS = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'").withZone(ZoneOffset.UTC);
    private static final Gson gson = new Gson();

    private static final ConcurrentHashMap<String, Set<Session>> resortSubscribers = new ConcurrentHashMap<>(); // key: resort
    // resortID -> dayID -> latest leaderboard received from the consumer
    private static final Map<String, Map<Integer, Leaderboard>> latest = new ConcurrentHashMap<>();

    @OnOpen
    public void onOpen(Session session, @PathParam("resortID") String resortID) {
        resortSubscribers.computeIfAbsent(resortID, k -> ConcurrentHashMap.newKeySet()).add(session);
        Map<Integer, Leaderboard> boards = latest.get(resortID);
        if (boards != null) {
            for (Leaderboard b : boards.values()) {
                send(session, toJson(b));
            }
        }
    }

    @OnClose
    public void onClose(Session session, @PathParam("resortID") String resortID) {
        Optional.ofNullable(resortSubscribers.get(resortID)).ifPresent(s -> s.remove(session));
    }

    @OnError
    public void onError(Session session, Throwable t) {
        System.err.println("WebSocket error on session " + session.getId() + ": " + t);
        for (Set<Session> sessions : resortSubscribers.values()) {
            sessions.remove(session);
        }
    }

    /** Forwards one update from the consumer to this server's subscribers for the resort. */
    public static void broadcast(int resortID, int dayID, long version, long generatedAtMillis,
                                 List<Leaderboard.Record> top) {
        String resort = Integer.toString(resortID);
        Leaderboard board = new Leaderboard(resort, dayID, version,
                ISO_MILLIS.format(Instant.ofEpochMilli(generatedAtMillis)), null, top);
        latest.computeIfAbsent(resort, k -> new ConcurrentHashMap<>()).put(dayID, board);

        Set<Session> sessions = resortSubscribers.get(resort);
        if (sessions == null || sessions.isEmpty()) {
            return;
        }
        String json = toJson(board);
        for (Session s : sessions) {
            send(s, json);
        }
    }

    // Stamps serverTimestamp with the send time, which the client uses for push latency.
    private static String toJson(Leaderboard b) {
        return gson.toJson(new Leaderboard(b.getResortID(), b.getDayID(), b.getVersion(), b.getGeneratedAt(),
                ISO_MILLIS.format(Instant.now()), b.getTop()));
    }

    // Blocking send, one at a time per session: Tomcat rejects overlapping writes on a session.
    private static void send(Session session, String json) {
        if (!session.isOpen()) {
            return;
        }
        synchronized (session) {
            try {
                session.getBasicRemote().sendText(json);
            } catch (IOException | IllegalStateException e) {
                // Closed mid-send; onClose/onError removes the session.
            }
        }
    }
}
