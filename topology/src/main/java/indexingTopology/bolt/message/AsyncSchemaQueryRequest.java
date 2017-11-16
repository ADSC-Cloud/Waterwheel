package indexingTopology.bolt.message;

/**
 * Created by robert on 16/11/17.
 */
public class AsyncSchemaQueryRequest extends AsyncRequestMessage {
    public AsyncSchemaQueryRequest(String name) {
        this.name = name;
    }
    public String name;
}
