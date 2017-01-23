package indexingTopology.util;

import indexingTopology.DataSchema;
import javafx.util.Pair;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.tuple.Tuple;

import java.util.concurrent.ArrayBlockingQueue;

/**
 * Created by acelzj on 13/1/17.
 */
public class IndexerBuilder {

    private ArrayBlockingQueue<Pair> queryPendingQueue;

    private ArrayBlockingQueue<Tuple> inputQueue;

    private OutputCollector collector;

    private String indexField;

    private DataSchema schema;

    int taskId;

    public IndexerBuilder setTaskId(int taskId) {
        this.taskId = taskId;
        return this;
    }

    public IndexerBuilder setOutputCollector(OutputCollector collector) {
        this.collector = collector;
        return this;
    }

    public IndexerBuilder setDataSchema(DataSchema schema) {
        this.schema = schema;
        return this;
    }

    public IndexerBuilder setInputQueue(ArrayBlockingQueue<Tuple> inputQueue) {
        this.inputQueue = inputQueue;
        return this;
    }

    public IndexerBuilder setQueryPendingQueue(ArrayBlockingQueue<Pair> queryPendingQueue) {
        this.queryPendingQueue = queryPendingQueue;
        return this;
    }

    public IndexerBuilder setIndexField(String indexField) {
        this.indexField = indexField;
        return this;
    }

    public Indexer getIndexer() {
        return new Indexer(taskId, inputQueue, indexField, schema, collector, queryPendingQueue);
    }
}
