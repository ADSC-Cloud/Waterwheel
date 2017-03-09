package indexingTopology.client;

import indexingTopology.data.PartialQueryResult;

/**
 * Created by robert on 8/3/17.
 */
public class QueryResponse extends PartialQueryResult {

    final Long queryId;

    public QueryResponse(PartialQueryResult partialQueryResult, Long queryId) {
        super(partialQueryResult);
        this.queryId = queryId;
    }

    public String toString() {
        return "QueryResponse: " + super.toString();
    }
}
