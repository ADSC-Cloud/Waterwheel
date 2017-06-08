package indexingTopology.api.client;


/**
 * Created by robert on 8/3/17.
 */
public class MessageResponse implements IResponse {
    String message;
    public MessageResponse(String message) {
        this.message = message;
    }
    public String toString() {
        return "Response: " + message;
    }
}
