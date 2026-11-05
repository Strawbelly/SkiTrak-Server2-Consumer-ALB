package server.model;

import java.util.List;

public class Leaderboard {
    private String resortID;
    private Integer dayID;
    private Long version;          // per-board change counter from the consumer
    private String generatedAt;    // when the consumer took the snapshot (UTC)
    private String serverTimestamp; // when this server sent it (UTC)
    private List<Record> top;

    public Leaderboard(String resortID, String serverTimestamp, List<Record> top) {
        this.resortID = resortID;
        this.serverTimestamp = serverTimestamp;
        this.top = top;
    }

    public Leaderboard(String resortID, int dayID, long version, String generatedAt,
                       String serverTimestamp, List<Record> top) {
        this(resortID, serverTimestamp, top);
        this.dayID = dayID;
        this.version = version;
        this.generatedAt = generatedAt;
    }

    public String getResortID() { return resortID; }
    public Integer getDayID() { return dayID; }
    public Long getVersion() { return version; }
    public String getGeneratedAt() { return generatedAt; }
    public String getServerTimestamp() { return serverTimestamp; }
    public List<Record> getTop() { return top; }

    public static class Record {
        private int rank;
        private int skierID;
        private int vertical;

        public Record(int rank, int skierID, int vertical) {
            this.rank = rank;
            this.skierID = skierID;
            this.vertical = vertical;
        }

        public int getRank() { return rank; }
        public int getSkierID() { return skierID; }
        public int getVertical() { return vertical; }
    }
}
