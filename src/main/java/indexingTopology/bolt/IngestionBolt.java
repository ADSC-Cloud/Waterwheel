package indexingTopology.bolt;

import com.esotericsoftware.kryo.Kryo;
import indexingTopology.DataTuple;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import indexingTopology.DataSchema;
import indexingTopology.streams.Streams;
import indexingTopology.util.*;
import javafx.util.Pair;

import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;

/**
 * Created by acelzj on 11/15/16.
 */
public class IngestionBolt<DataType extends Comparable> extends BaseRichBolt {
    private final DataSchema schema;

    private final String indexField;

    private OutputCollector collector;

    private Kryo kryo;

    private IndexerBuilder indexerBuilder;

    private Indexer indexer;

    private ArrayBlockingQueue<DataTuple> inputQueue;

    private ArrayBlockingQueue<SubQuery> queryPendingQueue;

    public IngestionBolt(String indexField, DataSchema schema) {
        this.schema = schema;
        this.indexField = indexField;
    }
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        collector = outputCollector;

        kryo = new Kryo();
        kryo.register(BTree.class, new KryoTemplateSerializer());
        kryo.register(BTreeLeafNode.class, new KryoLeafNodeSerializer());

        this.inputQueue = new ArrayBlockingQueue<>(1024);

        this.queryPendingQueue = new ArrayBlockingQueue<>(1024);

        indexerBuilder = new IndexerBuilder();

        indexer = indexerBuilder
                .setTaskId(topologyContext.getThisTaskId())
                .setDataSchema(schema)
                .setIndexField(indexField)
                .setInputQueue(inputQueue)
                .setQueryPendingQueue(queryPendingQueue)
                .setOutputCollector(collector)
                .getIndexer();
    }

    @Override
    public void cleanup() {
        super.cleanup();
    }

    public void execute(Tuple tuple) {
        if (tuple.getSourceStreamId().equals(Streams.IndexStream)) {
            DataTuple dataTuple = (DataTuple) tuple.getValueByField("tuple");
            try {
                inputQueue.put(dataTuple);
            } catch (InterruptedException e) {
                e.printStackTrace();
            } finally {
                collector.ack(tuple);
            }
        } else if (tuple.getSourceStreamId().equals(Streams.BPlusTreeQueryStream)){
            SubQuery subQuery = (SubQuery) tuple.getValueByField("subquery");
            try {
                queryPendingQueue.put(subQuery);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        } else if (tuple.getSourceStreamId().equals(Streams.TreeCleanStream)) {
            TimeDomain timeDomain = (TimeDomain) tuple.getValueByField("timeDomain");
            KeyDomain keyDomain = (KeyDomain) tuple.getValueByField("keyDomain");
            indexer.cleanTree(new Domain(keyDomain, timeDomain));
        }
    }

    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declareStream(Streams.FileInformationUpdateStream,
                new Fields("fileName", "keyDomain", "timeDomain"));

        outputFieldsDeclarer.declareStream(Streams.BPlusTreeQueryStream,
                new Fields("queryId", "serializedTuples"));

        outputFieldsDeclarer.declareStream(Streams.TimestampUpdateStream,
                new Fields("timeDomain", "keyDomain"));
    }
}
