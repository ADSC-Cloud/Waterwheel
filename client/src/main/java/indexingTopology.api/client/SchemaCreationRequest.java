package indexingTopology.api.client;

import indexingTopology.common.data.DataSchema;

/**
 * Created by robert on 16/11/17.
 */
public class SchemaCreationRequest extends IClientRequest {
    public SchemaCreationRequest(String name, DataSchema schema) {
        this.name = name;
        this.schema = schema;
    }
    public String name;
    public DataSchema schema;
}
