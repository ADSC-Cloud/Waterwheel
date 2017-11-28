package indexingTopology.api.client;

/**
 * Created by robert on 16/11/17.
 */
public class SchemaCreationResponse implements IResponse {
    public SchemaCreationResponse(int status) {
        this.status = status;
    }
    int status;
}
