package indexingTopology.metadata;

import indexingTopology.common.data.DataSchema;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by Robert on 8/2/17.
 */
public class SchemaManager {
    private Map<String, DataSchema> nameToSchema = new HashMap<>();

    public void setDefaultSchema(DataSchema schema) {
        nameToSchema.put("default", schema);
    }

    public DataSchema getDefaultSchema() {
        return nameToSchema.get("default");
    }

    public void createSchema(String name, DataSchema schema) {
        nameToSchema.put(name, schema);
    }

    public DataSchema getSchema(String name) {
        return nameToSchema.get(name);
    }
}
