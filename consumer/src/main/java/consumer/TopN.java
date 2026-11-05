package consumer;

import consumer.model.LeaderboardUpdate;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeSet;

/**
 * Maintains thread-safe Top-N standings for one resort and day.
 * Rankings use descending vertical, with lower skier IDs breaking ties.
 */
final class TopN {

    private static final Comparator<Standing> RANKING = Comparator
            .comparingLong((Standing s) -> s.vertical).reversed()
            .thenComparingInt(s -> s.skierID);

    private final int n;
    private final TreeSet<Standing> top = new TreeSet<>(RANKING);
    private final Map<Integer, Standing> inTop = new HashMap<>();
    private long version;

    TopN(int n) {
        this.n = n;
    }

    /** Updates a skier's standing and returns whether the leaderboard changed. */
    synchronized boolean update(int skierID, long totalVertical) {
        Standing updated = new Standing(skierID, totalVertical);
        Standing current = inTop.get(skierID);

        if (current != null) {
            // Totals only grow. Concurrent workers can apply one skier's swipes out of
            // order, so a total not above the current one is stale and is ignored.
            if (totalVertical <= current.vertical) {
                return false;
            }
            top.remove(current);
        } else if (top.size() >= n) {
            Standing last = top.last();
            if (RANKING.compare(updated, last) >= 0) {
                return false;
            }
            top.pollLast();
            inTop.remove(last.skierID);
        }

        top.add(updated);
        inTop.put(skierID, updated);
        version++;
        return true;
    }

    synchronized long version() {
        return version;
    }

    /** Returns the current standings with rank numbers assigned. */
    synchronized List<LeaderboardUpdate.Entry> entries() {
        List<LeaderboardUpdate.Entry> entries = new ArrayList<>(top.size());
        int rank = 1;

        for (Standing s : top) {
            entries.add(new LeaderboardUpdate.Entry(rank++, s.skierID, s.vertical));
        }

        return entries;
    }

    private static final class Standing {
        final int skierID;
        final long vertical;

        Standing(int skierID, long vertical) {
            this.skierID = skierID;
            this.vertical = vertical;
        }
    }
}
