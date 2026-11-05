package consumer;

import consumer.model.LeaderboardUpdate;
import consumer.model.SwipeEvent;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Aggregates swipe events and maintains Top-N leaderboards in memory.
 * Tracks skier vertical, leaderboard standings, and recent event IDs
 * for deduplication. State is shared across consumer workers.
 */
public class LeaderboardAggregator {

    public enum Outcome {
        CHANGED,
        UNCHANGED,
        DUPLICATE
    }

    // Key: resortID:dayID:skierID
    final ConcurrentHashMap<String, Long> skierVertical = new ConcurrentHashMap<>();

    // Key: resortID:dayID
    final ConcurrentHashMap<String, TopN> resortLeaderboards = new ConcurrentHashMap<>();

    final AtomicLong swipesProcessed = new AtomicLong();

    // Recent event IDs per resort, with a bounded deduplication window.
    private final ConcurrentHashMap<Integer, Map<String, Boolean>> recentEventIds =
            new ConcurrentHashMap<>();

    private final int topN;
    private final int dedupWindow;

    public LeaderboardAggregator(int topN, int dedupWindowPerResort) {
        this.topN = topN;
        this.dedupWindow = dedupWindowPerResort;
    }

    /** Applies a swipe once and reports whether the leaderboard changed. */
    public Outcome apply(SwipeEvent e) {
        Map<String, Boolean> seen = recentEventIds.computeIfAbsent(
                e.getResortID(), r -> boundedSet(dedupWindow));

        // Check and mark in one atomic step, so concurrent deliveries of the same
        // event on different workers apply it only once.
        if (seen.putIfAbsent(e.getEventId(), Boolean.TRUE) != null) {
            return Outcome.DUPLICATE;
        }

        boolean changed;
        try {
            long total = skierVertical.merge(
                    skierKey(e.getResortID(), e.getDayID(), e.getSkierID()),
                    (long) e.getVertical(),
                    Long::sum);

            changed = resortLeaderboards
                    .computeIfAbsent(
                            boardKey(e.getResortID(), e.getDayID()),
                            k -> new TopN(topN))
                    .update(e.getSkierID(), total);
        } catch (RuntimeException ex) {
            // Unmark so the worker's retry is not mistaken for a duplicate.
            seen.remove(e.getEventId());
            throw ex;
        }

        swipesProcessed.incrementAndGet();

        return changed ? Outcome.CHANGED : Outcome.UNCHANGED;
    }

    /** Returns a snapshot of the current leaderboard for a resort and day. */
    public LeaderboardUpdate snapshot(int resortID, int dayID) {
        TopN board = resortLeaderboards.get(boardKey(resortID, dayID));
        return new LeaderboardUpdate(
                resortID, dayID, board.version(),
                System.currentTimeMillis(), board.entries());
    }

    /** Returns a skier's accumulated vertical, or zero if no data exists. */
    public long totalVertical(int resortID, int dayID, int skierID) {
        return skierVertical.getOrDefault(
                skierKey(resortID, dayID, skierID), 0L);
    }

    static String skierKey(int resortID, int dayID, int skierID) {
        return resortID + ":" + dayID + ":" + skierID;
    }

    static String boardKey(int resortID, int dayID) {
        return resortID + ":" + dayID;
    }

    /** Creates a bounded insertion-ordered map that evicts the oldest event ID. */
    private static Map<String, Boolean> boundedSet(int capacity) {
        return Collections.synchronizedMap(new LinkedHashMap<String, Boolean>() {
            @Override
            protected boolean removeEldestEntry(Map.Entry<String, Boolean> eldest) {
                return size() > capacity;
            }
        });
    }
}
