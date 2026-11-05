package consumer.model;

/**
 * Swipe event published by the server to {@code skitrak.exchange}.
 * Mirrors server.model.SwipeEvent; modules do not share code, like the two clients.
 */
public class SwipeEvent {
    private String eventId;
    private int resortID;
    private int dayID;
    private int skierID;
    private int liftID;
    private int vertical;
    private int time;
    private long timestamp;
    private String serverId;

    public SwipeEvent() {}

    public SwipeEvent(String eventId, int resortID, int dayID, int skierID, int liftID,
                      int vertical, int time, long timestamp, String serverId) {
        this.eventId = eventId;
        this.resortID = resortID;
        this.dayID = dayID;
        this.skierID = skierID;
        this.liftID = liftID;
        this.vertical = vertical;
        this.time = time;
        this.timestamp = timestamp;
        this.serverId = serverId;
    }

    public String getEventId() { return eventId; }
    public int getResortID() { return resortID; }
    public int getDayID() { return dayID; }
    public int getSkierID() { return skierID; }
    public int getLiftID() { return liftID; }
    public int getVertical() { return vertical; }
    public int getTime() { return time; }
    public long getTimestamp() { return timestamp; }
    public String getServerId() { return serverId; }
}
