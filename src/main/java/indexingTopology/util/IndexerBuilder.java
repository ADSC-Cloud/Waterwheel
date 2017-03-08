package indexingTopology.util;

import indexingTopology.data.DataSchema;
import indexingTopology.data.DataTuple;

import java.util.concurrent.ArrayBlockingQueue;

/**
 * Created by acelzj on 13/1/17.
 */
public class IndexerBuilder {

    private ArrayBlockingQueue<SubQuery> queryPendingQueue;

    private ArrayBlockingQueue<DataTuple> inputQueue;

    private DataSchema schema;

    int taskId;

    public IndexerBuilder setTaskId(int taskId) {
        this.taskId = taskId;
        return this;
    }

    public IndexerBuilder setDataSchema(DataSchema schema) {
        this.schema = schema.duplicate();
        return this;
    }

    public IndexerBuilder setInputQueue(ArrayBlockingQueue<DataTuple> inputQueue) {
        this.inputQueue = inputQueue;
        return this;
    }

    public IndexerBuilder setQueryPendingQueue(ArrayBlockingQueue<SubQuery> queryPendingQueue) {
        this.queryPendingQueue = queryPendingQueue;
        return this;
    }

    public Indexer getIndexer() {
        return new Indexer(taskId, inputQueue, schema, queryPendingQueue);
    }
}
