package indexingTopology.bolt;

import indexingTopology.DataSchema;
import indexingTopology.util.*;
import indexingTopology.util.texi.Car;
import indexingTopology.util.texi.City;
import indexingTopology.util.texi.TrajectoryGenerator;
import indexingTopology.util.texi.TrajectoryUniformGenerator;
import javafx.util.Pair;
import org.apache.storm.tuple.Values;

import java.io.*;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;

/**
 * Created by acelzj on 12/23/16.
 */
public class IndexServerTest {

    private final static int numberOfIndexingThreads = 3;

    private BufferedReader bufferedReader;

    private static DataSchema schema;

    private final int btreeOrder;
    private static int bytesLimit;

    private final String indexField;

    private static int chunkId;

    private static boolean isTreeBuilt = false;

    private static MemChunk chunk;

    private TimingModule tm;
    private static SplitCounterModule sm;

    private long processingTime;

    private long totalTime;

    private static TemplateUpdater templateUpdater;

    private Double minIndexValue = Double.MAX_VALUE;
    private Double maxIndexValue = Double.MIN_VALUE;

    private Long minTimeStamp = Long.MAX_VALUE;
    private Long maxTimeStamp = Long.MIN_VALUE;


    private static ArrayBlockingQueue<Pair> inputQueue;

    private static BTree indexedData;

//    private static List<Thread> indexingThreads = new ArrayList<Thread>();

//    private static IndexingRunnable indexingRunnable;

    private static Indexer indexer;

    static List<String> fieldNames = new ArrayList<String>(Arrays.asList("id", "zcode", "payload"));
    static List<Class> valueTypes = new ArrayList<Class>(Arrays.asList(Double.class, Double.class, String.class));

    public IndexServerTest(String indexField, DataSchema schema, int btreeOrder, int bytesLimit) {
        this.indexField = indexField;
        this.schema = schema;
        this.btreeOrder = btreeOrder;
        this.bytesLimit = bytesLimit;
        this.inputQueue = new ArrayBlockingQueue<Pair>(1024);
        this.tm = TimingModule.createNew();
        this.sm = SplitCounterModule.createNew();
        this.chunk = MemChunk.createNew(bytesLimit);
        this.templateUpdater = new TemplateUpdater(btreeOrder, tm, sm);
        indexedData = new BTree(btreeOrder, tm, sm);
        indexer = new Indexer(inputQueue, indexedData, chunk);
    }


    private static void createGenerateThread() {
        Thread generateThread = new Thread(new GenerateRunnable());
        generateThread.start();
    }

    public static void main(String[] args) {

        DataSchema schema = new DataSchema(fieldNames, valueTypes, "user_id");

        final int btreeOrder = 4;

        final int bytesLimit = 65000000;

        IndexServerTest indexServerTest = new IndexServerTest("user_id", schema, btreeOrder, bytesLimit);

        createGenerateThread();

    }

    static class GenerateRunnable implements Runnable {
        @Override
        public void run() {
            final double x1 = 0;
            final double x2 = 1000;
            final double y1 = 0;
            final double y2 = 500;
            final int partitions = 100;

            final int payloadSize = 10;

            int numTuples = 0;

            TrajectoryGenerator generator = new TrajectoryUniformGenerator(10000, x1, x2, y1, y2);
            City city = new City(x1, x2, y1, y2, partitions);
            while (true) {
                Car car = generator.generate();
                try {
                    int ZCode = city.getZCodeForALocation(car.x, car.y);
                    Long timestamp = System.currentTimeMillis();
                    Values values = new Values((double) car.id, (double) ZCode,
                            new String(new char[payloadSize]), timestamp);
                    byte[] serializedTuples = serializeValues(values);

                    inputQueue.put(new Pair((double) ZCode, serializedTuples));

                } catch (InterruptedException e) {
                    e.printStackTrace();
                } catch (IOException e) {
                    e.printStackTrace();
                }

            }

        }

    }


    public static byte[] serializeValues(Values values) throws IOException {
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        for (int i=0;i < valueTypes.size();i++) {
            if (valueTypes.get(i).equals(Double.class)) {
                byte [] b = ByteBuffer.allocate(Double.SIZE / Byte.SIZE).putDouble((Double) values.get(i)).array();
                bos.write(b);
            } else if (valueTypes.get(i).equals(String.class)) {
                byte [] b = ((String) values.get(i)).getBytes();
                byte [] sizeHeader = ByteBuffer.allocate(Integer.SIZE/ Byte.SIZE).putInt(b.length).array();
                bos.write(sizeHeader);
                bos.write(b);
            } else {
                throw new IOException("Only classes supported till now are string and double");
            }
        }

        //As we add timestamp for a field, so we need to serialize the timestamp
        byte [] b = ByteBuffer.allocate(Long.SIZE / Byte.SIZE).putLong((Long) values.get(valueTypes.size())).array();
        bos.write(b);

        return bos.toByteArray();
    }

    public static Values deserialize(byte [] b) throws IOException {
        Values values = new Values();
        int offset = 0;
        for (int i = 0; i < valueTypes.size(); i++) {
            if (valueTypes.get(i).equals(Double.class)) {
                int len = Double.SIZE/Byte.SIZE;
                double val = ByteBuffer.wrap(b, offset, len).getDouble();
                values.add(val);
                offset += len;
            } else if (valueTypes.get(i).equals(String.class)) {
                int len = Integer.SIZE/Byte.SIZE;
                int sizeHeader = ByteBuffer.wrap(b, offset, len).getInt();
                offset += len;
                len = sizeHeader;
                String val = new String(b, offset, len);
                values.add(val);
                offset += len;
            } else {
                throw new IOException("Only classes supported till now are string and double");
            }
        }

        int len = Long.SIZE / Byte.SIZE;
        Long val = ByteBuffer.wrap(b, offset, len).getLong();
        values.add(val);
        return values;
    }

}
