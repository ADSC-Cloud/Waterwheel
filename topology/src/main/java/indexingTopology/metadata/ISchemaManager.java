package indexingTopology.metadata;

import indexingTopology.common.data.DataSchema;

/**
 * Created by robert on 16/11/17.
 */
public interface ISchemaManager {
    boolean createSchema(String name, DataSchema schema);

    DataSchema getSchema(String name);
}
