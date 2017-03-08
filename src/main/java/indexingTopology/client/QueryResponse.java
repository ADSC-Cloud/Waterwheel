package indexingTopology.client;

import indexingTopology.data.PartialQueryResult;

/**
 * Created by robert on 8/3/17.
 */
public class QueryResponse extends PartialQueryResult {
    public QueryResponse(PartialQueryResult partialQueryResult) {
        super(partialQueryResult);
    }

    public String toString() {
        return "QueryResponse: " + super.toString();
    }
}
