package indexingTopology.index;

import indexingTopology.common.*;
import indexingTopology.common.data.DataSchema;
import indexingTopology.common.data.DataTuple;
import indexingTopology.common.data.TrackedDataTuple;
import indexingTopology.config.TopologyConfig;
import javafx.util.Pair;
import junit.framework.TestCase;
import org.apache.storm.shade.org.eclipse.jetty.util.ConcurrentHashSet;
import org.junit.Test;

import java.io.IOException;
import java.util.List;
import java.util.Observable;
import java.util.Observer;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * Created by acelzj on 20/3/17.
 */
public class IndexerTest extends TestCase implements Observer{

    private Observable observable;
    private TopologyConfig config = new TopologyConfig();

    private Set<Long> tupleIdToBeAcked;

    public void setUp() {
        try {
            Runtime.getRuntime().exec("mkdir -p ./target/tmp");
        } catch (IOException e) {
            e.printStackTrace();
        }
        config.dataChunkDir = "./target/tmp";
        config.HDFSFlag = false;
        System.out.println("dataChunkDir is set to " + config.dataChunkDir);
        tupleIdToBeAcked = new ConcurrentHashSet<>();
    }

    public void tearDown() {
        try {
            Runtime.getRuntime().exec("rm ./target/tmp/*");
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void testIndexLogic() throws InterruptedException {
        config.dataChunkDir = "./target/tmp";
        config.CHUNK_SIZE = 58000000 ;
        IndexerBuilder indexerBuilder = new IndexerBuilder(config);

        LinkedBlockingQueue<DataTuple> inputQueue = new LinkedBlockingQueue<DataTuple>(1024);

        ArrayBlockingQueue<SubQuery> queryPendingQueue = new ArrayBlockingQueue<SubQuery>(1024);

        final int payloadSize = 1;
        DataSchema schema = new DataSchema();
        schema.addLongField("id");
        schema.addIntField("zcode");
        schema.addVarcharField("payload", payloadSize);
        schema.setPrimaryIndexField("zcode");
        schema.addLongField("timestamp");
        schema.setTemporalField("timestamp");

        DataSchema schemaWithTimestamp = schema.duplicate();

        Indexer indexer = indexerBuilder
                .setTaskId(0)
                .setDataSchema(schemaWithTimestamp)
                .setInputQueue(inputQueue)
                .setQueryPendingQueue(queryPendingQueue)
                .getIndexer();

        this.observable = indexer;
        observable.addObserver(this);

        int numberOfTuples = 10000;

//        System.out.println("sed -i 's/${JAVA_HOME}/\\/usr\\/lib\\/jvm/\\/jdk1.8.0_112/g' hadoop-env.sh");

        Long timestamp = 0L;
        for (int i = 0; i < numberOfTuples; ++i) {
            DataTuple dataTuple = new DataTuple((long) i, i, new String(new char[payloadSize]), timestamp);
            inputQueue.put(dataTuple);
        }


        Thread.sleep(1000);

        Long queryId = 0L;

        for (int i = 0; i < numberOfTuples; ++i) {
            SubQuery subQuery = new SubQuery(queryId, i, i, 0L, Long.MAX_VALUE, null);
            queryPendingQueue.put(subQuery);
            ++queryId;
        }
    }

    @Test
    public void testAck() throws InterruptedException {
        TopologyConfig config = new TopologyConfig();
        config.dataChunkDir = "./target/tmp";
        config.CHUNK_SIZE = 58000000 / 16;
        IndexerBuilder indexerBuilder = new IndexerBuilder(config);

        LinkedBlockingQueue<DataTuple> inputQueue = new LinkedBlockingQueue<>(1);

        ArrayBlockingQueue<SubQuery> queryPendingQueue = new ArrayBlockingQueue<SubQuery>(1024);

        final int payloadSize = 1;
        DataSchema schema = new DataSchema();
        schema.addLongField("id");
        schema.addIntField("zcode");
        schema.addVarcharField("payload", payloadSize);
        schema.setPrimaryIndexField("zcode");

        schema.addLongField("timestamp");
        schema.setTemporalField("timestamp");
        DataSchema schemaWithTimestamp = schema.duplicate();

        Indexer indexer = indexerBuilder
                .setTaskId(0)
                .setDataSchema(schemaWithTimestamp)
                .setInputQueue(inputQueue)
                .setQueryPendingQueue(queryPendingQueue)
                .getIndexer();

        this.observable = indexer;
        observable.addObserver(this);
        int numberOfTuples = 10000;

        Long timestamp = 0L;
        for (int i = 0; i < numberOfTuples; ++i) {
            DataTuple dataTuple = new DataTuple((long) i, i, new String(new char[payloadSize]), timestamp);
            tupleIdToBeAcked.add((long)i);
            inputQueue.put(new TrackedDataTuple((long)i, 0, dataTuple));
        }


        Thread.sleep(1000);

        assertEquals(0, tupleIdToBeAcked.size());
    }


    @Override
    public void update(Observable o, Object arg) {
        if (o instanceof Indexer) {
            String s = (String) arg;
            if (s.equals("information update")) {
                FileInformation fileInformation = ((Indexer) o).getFileInformation();
                String fileName = fileInformation.getFileName();
                Domain domain = fileInformation.getDomain();
                KeyDomain keyDomain = new KeyDomain(domain.getLowerBound(), domain.getUpperBound());
                TimeDomain timeDomain = new TimeDomain(domain.getStartTimestamp(), domain.getEndTimestamp());

                try {
                    Thread.sleep(100);
                    ((Indexer) o).cleanTree(domain);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }

            } else if (s.equals("query result")) {
                Pair pair = ((Indexer) o).getQueryResult();
                SubQuery subQuery = (SubQuery) pair.getKey();
                List<byte[]> queryResults = (List<byte[]>) pair.getValue();

                assertEquals(1, queryResults.size());
            } else if (s.equals("ack")) {
                try {
                    TrackedDataTuple tuple = ((Indexer) o).getTrackedDataTuple();
                    tupleIdToBeAcked.remove(tuple.tupleId);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }
    }
}