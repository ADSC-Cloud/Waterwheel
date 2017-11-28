package indexingTopology.bolt.message;

import indexingTopology.common.data.DataSchema;

/**
 * Created by robert on 16/11/17.
 */
public class AsyncSchemaQueryResponse extends AsyncResponseMessage {
    public AsyncSchemaQueryResponse(String name, DataSchema schema, long id) {
        this.name = name;
        this.schema = schema;
        this.id = id;
    }
    public String name;
    public DataSchema schema;
}
