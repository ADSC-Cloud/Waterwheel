package indexingTopology.api.client;

/**
 * Created by Robert on 8/2/17.
 */
public class SchemaQueryRequest extends IClientRequest {
    SchemaQueryRequest(String name) {
        this.name = name;
    }
    public String name;
}
