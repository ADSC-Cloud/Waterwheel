package indexingTopology.bolt;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import indexingTopology.aggregator.Aggregator;
import indexingTopology.bolt.metrics.LocationInfo;
import indexingTopology.cache.*;
import indexingTopology.config.TopologyConfig;
import indexingTopology.data.DataSchema;
import indexingTopology.data.DataTuple;
import indexingTopology.filesystem.FileSystemHandler;
import indexingTopology.filesystem.HdfsFileSystemHandler;
import indexingTopology.filesystem.LocalFileSystemHandler;
import indexingTopology.streams.Streams;
import indexingTopology.util.*;
import javafx.util.Pair;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.Semaphore;
import java.util.function.Predicate;
import java.util.stream.Collectors;

/**
 * Created by acelzj on 11/15/16.
 */
public class ChunkScanner <TKey extends Number & Comparable<TKey>> extends BaseRichBolt {

    OutputCollector collector;

    private transient LRUCache<String, byte[]> chunkNameToChunkMapping;

    private transient Kryo kryo;

    private DataSchema schema;

    private static final Logger LOG = LoggerFactory.getLogger(ChunkScanner.class);

    private transient ArrayBlockingQueue<SubQuery<TKey>> pendingQueue;

    private transient Semaphore subQueryHandlingSemaphore;

    private Thread subQueryHandlingThread;

    private Thread locationReportingThread;

    TopologyConfig config;

    private SubqueryHandler subqueryHandler;

    public ChunkScanner(DataSchema schema, TopologyConfig config) {
        this.schema = schema;
        this.config = config;
    }

    private Pair getTemplateFromExternalStorage(FileSystemHandler fileSystemHandler, String fileName) throws IOException {
//        fileSystemHandler.openFile("/", fileName);
        byte[] bytesToRead = new byte[4];
        fileSystemHandler.readBytesFromFile(0, bytesToRead);

        Input input = new Input(bytesToRead);
        int templateLength = input.readInt();


        bytesToRead = new byte[templateLength];
        fileSystemHandler.readBytesFromFile(4, bytesToRead);

        input = new Input(bytesToRead);
        BTree template = kryo.readObject(input, BTree.class);


//        fileSystemHandler.closeFile();

        return new Pair(template, templateLength);
    }

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        collector = outputCollector;

        chunkNameToChunkMapping = new LRUCache<>(config.CACHE_SIZE);

        pendingQueue = new ArrayBlockingQueue<>(1024);

        subQueryHandlingSemaphore = new Semaphore(1);

        createSubQueryHandlingThread();

//        kryo = new Kryo();
//        kryo.register(BTree.class, new KryoTemplateSerializer(config));
//        kryo.register(BTreeLeafNode.class, new KryoLeafNodeSerializer(config));
        subqueryHandler = new SubqueryHandler(schema, config);

        locationReportingThread = new Thread(() -> {
//            while (true) {
            while (!Thread.currentThread().isInterrupted()) {
                String hostName = "unknown";
                try {
                    hostName = InetAddress.getLocalHost().getHostName();
                } catch (UnknownHostException e) {
                    e.printStackTrace();
                }
                LocationInfo info = new LocationInfo(LocationInfo.Type.Query, topologyContext.getThisTaskId(), hostName);
                outputCollector.emit(Streams.LocationInfoUpdateStream, new Values(info));

                try {
                    Thread.sleep(10000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                    Thread.currentThread().interrupt();
                    break;
                }
            }
        });
        locationReportingThread.start();
    }

    @Override
    public void execute(Tuple tuple) {

        if (tuple.getSourceStreamId().equals(Streams.FileSystemQueryStream)) {

            SubQueryOnFile subQuery = (SubQueryOnFile) tuple.getValueByField("subquery");

            try {
//                System.out.println("query id " + subQuery.getQueryId() + " has been put to the queue");
                pendingQueue.put(subQuery);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        } else if (tuple.getSourceStreamId().equals(Streams.SubQueryReceivedStream)) {
            subQueryHandlingSemaphore.release();
//            System.out.println(Streams.SubQueryReceivedStream);
        }
//        LOG.info("Subquery time : " + (System.currentTimeMillis() - start));
    }

    private void createSubQueryHandlingThread() {
        subQueryHandlingThread = new Thread(new Runnable() {
            @Override
            public void run() {
//                while (true) {
                while (!Thread.currentThread().isInterrupted()) {
                    try {
                        subQueryHandlingSemaphore.acquire();
                        SubQueryOnFile subQuery = (SubQueryOnFile) pendingQueue.take();
//                        System.out.println("sub query " + subQuery.getQueryId() + " has been taken from queue");

                        List<byte[]> tuples = subqueryHandler.handleSubquery(subQuery);

                        collector.emit(Streams.FileSystemQueryStream, new Values(subQuery, tuples, new FileScanMetrics()));

                        if (!config.SHUFFLE_GROUPING_FLAG) {
                            collector.emit(Streams.FileSubQueryFinishStream, new Values("finished"));
                        }
//                        System.out.println("sub query " + subQuery.getQueryId() + " has been processed");
                    } catch (InterruptedException e) {
//                        e.printStackTrace();
                        Thread.currentThread().interrupt();
                        break;
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                }
            }
        });
        subQueryHandlingThread.start();
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declareStream(Streams.FileSystemQueryStream,
                new Fields("queryId", "serializedTuples", "metrics"));

        outputFieldsDeclarer.declareStream(Streams.FileSubQueryFinishStream,
                new Fields("finished"));

        outputFieldsDeclarer.declareStream(Streams.LocationInfoUpdateStream, new Fields("info"));
    }

    @Override
    public void cleanup() {
        super.cleanup();
        subQueryHandlingThread.interrupt();
        locationReportingThread.interrupt();
    }
}