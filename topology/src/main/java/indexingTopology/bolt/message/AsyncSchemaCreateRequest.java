package indexingTopology.bolt.message;

import indexingTopology.common.data.DataSchema;

/**
 * Created by robert on 16/11/17.
 */
public class AsyncSchemaCreateRequest extends AsyncRequestMessage {
    public String name;
    public DataSchema schema;
    public AsyncSchemaCreateRequest(String name, DataSchema schema) {
        this.name = name;
        this.schema = schema;
    }
}
