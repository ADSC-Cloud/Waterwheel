package indexingTopology.api.server;

import indexingTopology.api.client.SchemaCreationRequest;
import indexingTopology.api.client.SchemaQueryRequest;
import indexingTopology.common.data.DataSchema;

import java.io.IOException;

/**
 * Created by Robert on 8/2/17.
 */
public interface SchemaManipulationHandle {
    void handle(SchemaQueryRequest request) throws IOException;
    void handle(SchemaCreationRequest request) throws IOException;
}
