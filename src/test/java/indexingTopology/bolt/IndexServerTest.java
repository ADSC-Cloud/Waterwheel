package indexingTopology.bolt;

import com.esotericsoftware.kryo.io.Output;
import indexingTopology.data.DataSchema;
import indexingTopology.config.TopologyConfig;
import indexingTopology.util.*;
import indexingTopology.util.taxi.*;
import javafx.util.Pair;
import org.apache.storm.tuple.Values;
import org.apache.log4j.Logger;


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

    private static DataSchema schema;

    private final int btreeOrder;
    private static int bytesLimit;

    private final String indexField;

    private static int chunkId;

    private static boolean isTreeBuilt = false;

    private static MemChunk chunk;

    private long processingTime;

    private long totalTime;

    private static TemplateUpdater templateUpdater;

    private Double minIndexValue = Double.MAX_VALUE;
    private Double maxIndexValue = Double.MIN_VALUE;

    private Long minTimestamp = Long.MAX_VALUE;
    private Long maxTimestamp = Long.MIN_VALUE;

    private long queryId;


    private static ArrayBlockingQueue<Pair> inputQueue;

    private static BTree indexedData;

    private static IndexerCopy indexer;

    private static Logger logger;

    private static Thread generateThread;

    private static BufferedWriter bufferedWriter;

    private static GenerateRunnable generateRunnable;

    private static BufferedReader bufferedReader;

    static List<String> fieldNames = new ArrayList<String>(Arrays.asList("id", "zcode", "payload"));
    static List<Class> valueTypes = new ArrayList<Class>(Arrays.asList(Double.class, Double.class, String.class));

    public IndexServerTest(String indexField, DataSchema schema, int btreeOrder, int bytesLimit, boolean templateMode,
                           int numberOfIndexThreads, TopologyConfig config) throws InterruptedException {
        this.indexField = indexField;
        this.schema = schema;
        this.btreeOrder = btreeOrder;
        this.bytesLimit = bytesLimit;
        this.inputQueue = new ArrayBlockingQueue<>(config.PENDING_QUEUE_CAPACITY);
        this.chunk = MemChunk.createNew(bytesLimit);
        this.templateUpdater = new TemplateUpdater(btreeOrder, config);
        indexedData = new BTree(btreeOrder, config);
//        try {
//            bufferedReader = new BufferedReader(new FileReader(new File(TopologyConfig.dataDir + TopologyConfig.dataFileDir)));
//        } catch (FileNotFoundException e) {
//            e.printStackTrace();
//        }
//        queryId = 0;


//        try {
//          File file = new File("/home/acelzj/test_data/" + "gauss_data" + ".txt");
//            File file = new File("/home/acelzj/test_data/" + "zipf_data" + ".txt");
//            File file = new File("/home/acelzj/test_data/" + "uniform_data" + ".txt");
//            File file = new File("/home/acelzj/logs/" + btreeOrder + TopologyConfig.SKEWNESS_DETECTION_THRESHOLD + ".txt");
//            File file = new File("/home/acelzj/logs/" + 20048 + ".txt");
//            File file = new File(TopologyConfig.dataDir + TopologyConfig.logDir + "/" + numberOfIndexThreads + ".txt");
//
//            if (!file.exists()) {
//                file.createNewFile();
//                System.out.println(file.getName() + "has been created!!!");
//            }

//            FileWriter fileWriter = new FileWriter(file,true);

//            bufferedWriter = new BufferedWriter(fileWriter);
//        } catch (FileNotFoundException e) {
//            e.printStackTrace();
//        } catch (IOException e) {
//            e.printStackTrace();
//        }

        indexer = new IndexerCopy(0, inputQueue, indexedData, indexField, schema, bufferedWriter, btreeOrder, templateMode, numberOfIndexThreads, config);
    }

    private static void createGenerateThread() {
        if (generateRunnable == null) {
            generateRunnable = new GenerateRunnable();
        }
        generateThread = new Thread(generateRunnable);
        generateThread.start();
    }

    private static void terminateGenerateThread() throws InterruptedException {
        generateRunnable.setInputExhausted();
        generateThread.join();
        generateThread = null;
        generateRunnable = null;
    }

    public static void main(String[] args) throws InterruptedException {

        String indexField = "zcode";
        TopologyConfig config = new TopologyConfig();

        DataSchema schema = new DataSchema();
        schema.addDoubleField("id");
//        schema.addLongField("id");
        schema.addDoubleField("zcode");
//        schema.addIntField("zcode");
        schema.addVarcharField("payload", 10);
        schema.setPrimaryIndexField("zcode");

        final int btreeOrder = config.BTREE_ORDER;

        final int bytesLimit = 65000000;

        List<Integer> orders = new ArrayList<>();
        for (int i = 64; i <= 64; ++i) {
            orders.add(i);
        }

//        for (int i = 0; i < 2; ++i) {

        double threshold = 0.5;

            for (Integer order : orders) {
                for (int i = 1; i <= 4; i*=2) {
//                    for (int j = 0; j <= 1; ++j) {
//                        boolean templateMode = (j != 0);
                        boolean templateMode = true;
                    config.SKEWNESS_DETECTION_THRESHOLD = threshold;
                        IndexServerTest indexServerTest = new IndexServerTest(indexField, schema, order, bytesLimit, false, i, config);

                        createGenerateThread();

                        Thread.sleep(1 * 2000000 * 1000);

                        terminateGenerateThread();

//                        System.out.println("terminate generate thread!!!");

                        indexer.terminateInputProcessingThread();

//                        System.out.println("terminate input processing thread!!!");

//                        indexer.terminateQueryThreads();

                        threshold += 0.1;
                    }
                }
//        }

    }

    static class GenerateRunnable implements Runnable {


        boolean inputExhausted = false;

        public void setInputExhausted() {
            inputExhausted = true;
        }

        @Override
        public void run() {
            final double x1 = 0;
            final double x2 = 1000;
            final double y1 = 0;
            final double y2 = 500;
            final int partitions = 100;

            final int payloadSize = 10;

            int numTuples = 0;

            long timestamp = 0;

            TrajectoryGenerator generator = new TrajectoryUniformGenerator(10000, x1, x2, y1, y2);
//            RandomGenerator randomGenerator = new Well19937c();
//            randomGenerator.setSeed(1000);
//            KeyGenerator keyGenerator = new ZipfKeyGenerator( 200048, 0.3, randomGenerator);
//            KeyGenerator keyGenerator = new UniformKeyGenerator();
            City city = new City(x1, x2, y1, y2, partitions);
            while (true) {
                if (inputExhausted) {
                    break;
                }
                Car car = generator.generate();
//                KeyGenerator keyGenerator = new GaussKeyGenerator(10240, 500);
//                KeyGenerator keyGenerator = new UniformKeyGenerator();
                try {
//                    String text = bufferedReader.readLine();
//                    String [] tuple = text.split(" ");
//                    int ZCode = city.getZCodeForALocation(car.x, car.y);
//                    try {
//                        String text = "" + car.id + " " + keyGenerator.generate() + " " + new String(new char[payloadSize]) + " " + timestamp;
//                        bufferedWriter.write(text);
//                        bufferedWriter.newLine();
//                        bufferedWriter.flush();
//                    } catch (IOException e) {
//                        e.printStackTrace();
//                    }
//                    Double key = keyGenerator.generate();
                    double zcode = city.getZCodeForALocation(car.x, car.y);
                    List<Object> values = new ArrayList<>();
                    values.add((double) car.id);
                    values.add(zcode);
                    values.add(new String(new char[payloadSize]));
                    values.add(timestamp);
//
                    byte[] serializedTuples = serializeValues(values);

                    inputQueue.put(new Pair(zcode, serializedTuples));
//                    values.add(Double.parseDouble(tuple[0]));
//                    values.add(Double.parseDouble(tuple[1]));
//                    values.add(tuple[2]);
//                    values.add(Long.parseLong(tuple[3]));
//                    byte[] serializedTuples = serializeValues(values);
//                    inputQueue.put(new Pair(Double.parseDouble(tuple[1]), serializedTuples));
                    ++timestamp;
//
                } catch (InterruptedException e) {
                    e.printStackTrace();
                } catch (IOException e) {
                    e.printStackTrace();
                }

            }

        }

    }

    /*
    public static byte[] serializeValues(List<Object> values) throws IOException {
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
    */

    public static byte[] serializeValues(List<Object> values) throws IOException {
        Output output = new Output(1000, 2000000);
        for (int i = 0; i < valueTypes.size(); i++) {
            if (valueTypes.get(i).equals(Double.class)) {
                output.writeDouble((Double) values.get(i));
            } else if (valueTypes.get(i).equals(String.class)) {
                output.writeString((String) values.get(i));
            } else {
                throw new IOException("Only classes supported till now are string and double");
            }
        }

        //As we add timestamp for a field, so we need to serialize the timestamp
        output.writeLong((Long) values.get(valueTypes.size()));
        return output.toBytes();
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
