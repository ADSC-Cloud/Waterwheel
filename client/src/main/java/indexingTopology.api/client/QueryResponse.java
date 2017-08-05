package indexingTopology.api.client;

import java.util.List;
import indexingTopology.common.data.DataSchema;
import indexingTopology.common.data.DataTuple;
import indexingTopology.common.data.PartialQueryResult;

/**
 * Created by robert on 8/3/17.
 */
public class QueryResponse extends PartialQueryResult implements IResponse {

    final Long queryId;
    private DataSchema schema;

    public QueryResponse(PartialQueryResult partialQueryResult, DataSchema schema, Long queryId) {
        super(partialQueryResult);
        this.queryId = queryId;
        this.schema = schema;
    }

    public List<DataTuple> getTuples() {
        return dataTuples;
    }

    public String toString() {
        return "QueryResponse: " + super.toString();
    }

    public DataSchema getSchema() {
        return schema;
    }

    public void setSchema(DataSchema schema) {
        this.schema = schema;
    }
}
