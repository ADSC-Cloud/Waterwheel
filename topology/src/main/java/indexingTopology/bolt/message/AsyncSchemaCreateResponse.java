package indexingTopology.bolt.message;

/**
 * Created by robert on 16/11/17.
 */
public class AsyncSchemaCreateResponse extends AsyncResponseMessage {
    public boolean isSuccessful;
    public String name;
    public AsyncSchemaCreateResponse(String name, boolean isSuccessful, long id) {
        this.name = name;
        this.isSuccessful = isSuccessful;
        this.id = id;
    }
}
