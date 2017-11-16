package indexingTopology.metadata;

import indexingTopology.common.data.DataSchema;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by Robert on 8/2/17.
 */
public class SchemaManager implements ISchemaManager {
    private Map<String, DataSchema> nameToSchema = new HashMap<>();

    public void setDefaultSchema(DataSchema schema) {
        nameToSchema.put("default", schema);
    }

    public DataSchema getDefaultSchema() {
        return nameToSchema.get("default");
    }

    public boolean createSchema(String name, DataSchema schema) {
        if (schema ==null || nameToSchema.containsKey(name)) {
            System.out.println("Cannot create Schema " + name);
            return false;
        } else {
            nameToSchema.put(name, schema);
            return true;
        }
    }

    public DataSchema getSchema(String name) {
        final DataSchema schema = nameToSchema.get(name);
        return schema;
    }
}
