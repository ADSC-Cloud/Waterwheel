package indexingTopology.bolt;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Output;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.TupleImpl;
import org.apache.storm.tuple.Values;
import indexingTopology.config.TopologyConfig;
import indexingTopology.DataSchema;
import indexingTopology.filesystem.FileSystemHandler;
import indexingTopology.filesystem.HdfsFileSystemHandler;
import indexingTopology.filesystem.LocalFileSystemHandler;
import indexingTopology.streams.Streams;
import indexingTopology.exception.UnsupportedGenericException;
import indexingTopology.util.*;
import javafx.util.Pair;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;

/**
 * Created by acelzj on 11/15/16.
 */
public class IngestionBolt extends BaseRichBolt {
    private final DataSchema schema;

    private final String indexField;

    private OutputCollector collector;

    private Kryo kryo;

    private IndexerBuilder indexerBuilder;

    private Indexer indexer;

    private ArrayBlockingQueue<Tuple> inputQueue;

    private ArrayBlockingQueue<Pair> queryPendingQueue;

    public IngestionBolt(String indexField, DataSchema schema) {
        this.schema = schema;
        this.indexField = indexField;
    }
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        collector = outputCollector;

        kryo = new Kryo();
        kryo.register(BTree.class, new KryoTemplateSerializer());
        kryo.register(BTreeLeafNode.class, new KryoLeafNodeSerializer());

        this.inputQueue = new ArrayBlockingQueue<Tuple>(1024);

        this.queryPendingQueue = new ArrayBlockingQueue<Pair>(1024);

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
            try {
                inputQueue.put(tuple);
            } catch (InterruptedException e) {
                e.printStackTrace();
            } finally {
                collector.ack(tuple);
            }
        } else if (tuple.getSourceStreamId().equals(Streams.BPlusTreeQueryStream)){
            Long queryId = tuple.getLong(0);
            Double leftKey = tuple.getDouble(1);
            Double rightKey = tuple.getDouble(2);
            Pair pair = new Pair(queryId, new Pair(leftKey, rightKey));

            try {
                queryPendingQueue.put(pair);
                System.out.println("query id " + queryId + " has been put to pending queue!!!");
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        } else if (tuple.getSourceStreamId().equals(Streams.TreeCleanStream)) {
            Pair keyRange = (Pair) tuple.getValueByField("keyRange");
            Pair timestampRange = (Pair) tuple.getValueByField("timestampRange");
            Double keyRangeLowerBound = (Double) keyRange.getKey();
            Double keyRangeUpperBound = (Double) keyRange.getValue();
            Long startTimestamp = (Long) timestampRange.getKey();
            Long endTimestamp = (Long) timestampRange.getValue();
            indexer.cleanTree(new Domain(startTimestamp, endTimestamp, keyRangeLowerBound, keyRangeUpperBound));
        }
    }

    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declareStream(Streams.FileInformationUpdateStream,
                new Fields("fileName", "keyRange", "timeStampRange"));

        outputFieldsDeclarer.declareStream(Streams.BPlusTreeQueryStream,
                new Fields("queryId", "serializedTuples"));

        outputFieldsDeclarer.declareStream(Streams.TimeStampUpdateStream,
                new Fields("timestampRange", "keyRange"));
    }
}
