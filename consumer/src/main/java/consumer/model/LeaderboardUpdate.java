package consumer.model;

import java.util.List;

/**
 * Top-N snapshot for one (resortID, dayID), published to the {@code skitrak.leaderboard}
 * fanout exchange whenever the standings change.
 */
public class LeaderboardUpdate {
    private int resortID;
    private int dayID;
    private long version;       // increments on every standings change of this board
    private long generatedAt;   // epoch milliseconds when the snapshot was taken
    private List<Entry> top;

    public LeaderboardUpdate() {}

    public LeaderboardUpdate(int resortID, int dayID, long version, long generatedAt, List<Entry> top) {
        this.resortID = resortID;
        this.dayID = dayID;
        this.version = version;
        this.generatedAt = generatedAt;
        this.top = top;
    }

    public int getResortID() { return resortID; }
    public int getDayID() { return dayID; }
    public long getVersion() { return version; }
    public long getGeneratedAt() { return generatedAt; }
    public List<Entry> getTop() { return top; }

    public static class Entry {
        private int rank;
        private int skierID;
        private long vertical;

        public Entry() {}

        public Entry(int rank, int skierID, long vertical) {
            this.rank = rank;
            this.skierID = skierID;
            this.vertical = vertical;
        }

        public int getRank() { return rank; }
        public int getSkierID() { return skierID; }
        public long getVertical() { return vertical; }
    }
}
