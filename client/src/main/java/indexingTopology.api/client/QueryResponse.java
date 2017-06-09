package indexingTopology.api.client;

import indexingTopology.common.data.PartialQueryResult;

/**
 * Created by robert on 8/3/17.
 */
public class QueryResponse extends PartialQueryResult implements IResponse {

    final Long queryId;

    public QueryResponse(PartialQueryResult partialQueryResult, Long queryId) {
        super(partialQueryResult);
        this.queryId = queryId;
    }

    public String toString() {
        return "QueryResponse: " + super.toString();
    }
}
