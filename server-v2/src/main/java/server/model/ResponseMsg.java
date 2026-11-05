package server.model;

public class ResponseMsg {
    private String message;

    public ResponseMsg() {}

    public ResponseMsg(String message) {
        this.message = message;
    }

    public String getMessage() {
        return message;
    }
    public void setMessage(String message) {
        this.message = message;
    }
}