package indexingTopology.client;

import java.io.Serializable;

/**
 * Created by robert on 3/3/17.
 */
public class Response implements Serializable {
    String message;
    public String toString() {
        return "Response: " + message;
    }
}
