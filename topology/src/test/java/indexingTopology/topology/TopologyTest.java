package indexingTopology.topology;

import com.esotericsoftware.kryo.serializers.DefaultSerializers;
import indexingTopology.api.client.*;
import indexingTopology.bolt.*;

import indexingTopology.common.aggregator.*;
import indexingTopology.common.logics.DataTupleEquivalentPredicateHint;
import indexingTopology.common.logics.DataTupleMapper;
import indexingTopology.common.logics.DataTuplePredicate;
import indexingTopology.common.logics.DataTupleSorter;
import indexingTopology.config.TopologyConfig;
import indexingTopology.common.data.DataSchema;
import indexingTopology.common.data.DataTuple;
import indexingTopology.util.*;
import indexingTopology.util.shape.Point;
import indexingTopology.util.shape.Rectangle;
import indexingTopology.util.taxi.Car;
import indexingTopology.util.taxi.City;
import info.batey.kafka.unit.KafkaUnit;
import junit.framework.TestCase;
import kafka.producer.KeyedMessage;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.LongSerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.generated.KillOptions;
import org.apache.storm.generated.StormTopology;

import java.io.FileReader;
import java.io.IOException;
import java.io.Serializable;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.function.Function;

import org.junit.Test;


/**
 * Created by Robert on 5/15/17.
 */
public class TopologyTest extends TestCase {

    TopologyConfig config = new TopologyConfig();

    AvailableSocketPool socketPool = new AvailableSocketPool();

    LocalCluster cluster;

    Producer<String, String> producer = null;
    int totalNumber = 0;

    boolean setupDone = false;

    boolean tearDownDone = false;

    transient KafkaUnit kafkaUnitServer;

    public void setUp() {
        if (!setupDone) {
            try {
                Runtime.getRuntime().exec("mkdir -p ./target/tmp");
            } catch (IOException e) {
                e.printStackTrace();
            }
            config.dataChunkDir = "./target/tmp";
            config.metadataDir = "./target/tmp";
            config.CHUNK_SIZE = 2 * 1024 * 1024;
            config.HDFSFlag = false;
            config.HDFSFlag = false;
            config.previousTime = Integer.MAX_VALUE;
            System.out.println("dataChunkDir is set to " + config.dataChunkDir);
            cluster = new LocalCluster();
            setupDone = true;
        }
    }

    public void tearDown() {
        if (! tearDownDone) {
            try {
                Runtime.getRuntime().exec("rm ./target/tmp/*");
            } catch (IOException e) {
                e.printStackTrace();
            }
            cluster.shutdown();
            tearDownDone = true;
        }
    }

    @Test
    public void testKafkaTopologyKeyRangeQuery() throws InterruptedException, ClassNotFoundException {
        DataSchema rawSchema = new DataSchema();
        rawSchema.addDoubleField("lon");
        rawSchema.addDoubleField("lat");
        rawSchema.addIntField("devbtype");
        rawSchema.addVarcharField("devid", 8);
        rawSchema.addVarcharField("id", 32);

        DataSchema schema = new DataSchema();
        schema.addDoubleField("lon");
        schema.addDoubleField("lat");
        schema.addIntField("devbtype");
        schema.addVarcharField("devid", 8);
        schema.addVarcharField("id", 32);
        schema.addIntField("zcode");
        schema.addLongField("timestamp");
        schema.setPrimaryIndexField("zcode");

        int queryPort = socketPool.getAvailablePort();
//        int kafkaZkport = socketPool.getAvailablePort();
        int kafkaZkport = 2181;
        int kafkaUnitport = socketPool.getAvailablePort();

        kafkaUnitServer = new KafkaUnit("localhost:" + kafkaZkport,"localhost:" + kafkaUnitport);
        kafkaUnitServer.startup();
        Thread.sleep(1000);
        double x1 = 40.0;
        double x2 = 90.0;
        double y1 = 30.0;
        double y2 = 80.0;
        int partitions = 128;
        City city = new City(x1, x2, y1, y2, partitions);
        Integer lowerBound = 0;
        Integer upperBound = 5000;

        final int minIndex = 0;
        final int maxIndex = 99999;

        TopologyGenerator<Integer> topologyGenerator = new TopologyGenerator<>();

        assertTrue(config != null);


        int total = 100;
        Thread emittingThread;
        long start = System.currentTimeMillis();
        System.out.println("Kafka Producer send msg start,total msgs:"+total);

//        kafkaUnitServer.createTopic("consumer");
        // set up the producer
//        Properties props = new Properties();
////        props.put("bootstrap.servers", "localhost:9092");
//        props.put("group.id", 0);
//        props.put("acks", "all");
//        props.put("retries", "0");
//        props.put("batch.size", 16384);
//        props.put("auto.commit.interval.ms", "1000");
//        props.put("buffer.memory", 33554432);
//        props.put("key.serializer", StringSerializer.class.getName());
//        props.put("value.serializer", StringSerializer.class.getName());
////        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, LongSerializer.class.getCanonicalName());
////        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getCanonicalName());
//        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaUnitServer.getKafkaConnect());
//        producer = new KafkaProducer<String, String>(props);
        int meetRequirements = 0;
        for (int i = 0; i < total; i++) {
            totalNumber++;
            Long timestamp = System.currentTimeMillis();
            Double lon = Math.random() * 100;
            Double lat = Math.random() * 100;
            final int id = new Random().nextInt(100);
            final String idString = "" + id;
            if(lon >= x1 && lon <= x2 && lat >= y1 && lat <= y2){
                meetRequirements++;
            }
            KeyedMessage<String, String> keyedMessage = new KeyedMessage<>("consumer", "key", "{\"lon\":"+ lon + ",\"lat\":" + lat + ",\"devbtype\":"+ totalNumber +",\"devid\":\"asd\",\"id\":"+ idString +"}");
            kafkaUnitServer.sendMessages(keyedMessage);
//            this.producer.send(new ProducerRecord<String, String>("consumer",
//                    String.valueOf(i),
//                    "{\"lon\":"+ lon + ",\"lat\":" + lat + ",\"devbtype\":"+ totalNumber +",\"devid\":\"asd\",\"id\":"+ idString +"}"));
//            this.producer.flush();
        }
        System.out.println("Kafka Producer send msg over,cost time:" + (System.currentTimeMillis() - start) + "ms");
        Thread.sleep(5000);
        FakeKafkaReceiverBolt inputStreamReceiverBolt = new FakeKafkaReceiverBolt(rawSchema, config, kafkaUnitServer, total);

        QueryCoordinatorBolt<Integer> coordinator = new GeoTemporalQueryCoordinatorBoltBolt<>(lowerBound,
                upperBound, queryPort, city, config, schema);

        DataTupleMapper dataTupleMapper = new DataTupleMapper(rawSchema, (Serializable & Function<DataTuple, DataTuple>) t -> {
            double lon = (double)schema.getValue("lon", t);
            double lat = (double)schema.getValue("lat", t);
            int zcode = city.getZCodeForALocation(lon, lat);
            t.add(zcode);
            t.add(System.currentTimeMillis());
            return t;
        });



//        StormTopology topology = topologyGenerator.generateIndexingTopology(schema, lowerBound, upperBound,
//                enableLoadBalance, dataSource, queryCoordinatorBolt, dataTupleMapper, bloomFilterColumns, config);
        List<String> bloomFilterColumns = new ArrayList<>();
        bloomFilterColumns.add("id");

        topologyGenerator.setNumberOfNodes(1);

        StormTopology topology = topologyGenerator.generateIndexingTopology(schema, lowerBound, upperBound, false, inputStreamReceiverBolt,
                coordinator,dataTupleMapper, bloomFilterColumns , config);
        Config conf = new Config();
        conf.setDebug(false);
        conf.setNumWorkers(1);

        conf.put(Config.WORKER_CHILDOPTS, "-Xmx1024m");
        conf.put(Config.WORKER_HEAP_MEMORY_MB, 1024);
        conf.put(Config.STORM_MESSAGING_NETTY_MAX_SLEEP_MS, 1);

        // use ResourceAwareScheduler with some magic configurations to ensure that QueryCoordinator and Sink
        // are executed on the nimbus node.
        conf.setTopologyStrategy(org.apache.storm.scheduler.resource.strategies.scheduling.DefaultResourceAwareStrategy.class);

        cluster.submitTopology("testSimpleTopologyKeyRangeQuery", conf, topology);

        final int tuples = 1000;


        GeoTemporalQueryClient queryClient = new GeoTemporalQueryClient("localhost", queryPort);

        try {
            queryClient.connectWithTimeout(50000);
        } catch (IOException e) {
            e.printStackTrace();
        }

        DataTuplePredicate predicate = t -> new Rectangle(new Point(x1,y2),new Point(x2,y1)).checkIn(new Point((Double)schema.getValue("lon", t),(Double)schema.getValue("lat", t)));

//         wait for the tuples to be appended, because of the time and currentTimeMillis
        Thread.sleep(5000);

        GeoTemporalQueryRequest queryRequest = new GeoTemporalQueryRequest<>(x1, x2, y1, y2,
                System.currentTimeMillis() - 1000 * 1000,
                System.currentTimeMillis(), predicate, null, null, null, null);

        ExecutorService executorService = Executors.newCachedThreadPool();


        boolean fullyExecuted = false;



        try {

            QueryResponse response = queryClient.query(queryRequest);
            System.out.println(queryPort);
            System.out.println("meetRequirements:" + meetRequirements);
            System.out.println("dataTuples.size:" + response.dataTuples.size());
            assertEquals(meetRequirements, response.dataTuples.size());
            fullyExecuted = true;

        } catch (IOException e) {
            e.printStackTrace();
        }

        try {
            kafkaUnitServer.shutdown();
            queryClient.close();
            KillOptions killOptions = new KillOptions();
            killOptions.set_wait_secs(0);
            cluster.killTopologyWithOpts("testSimpleTopologyKeyRangeQuery", killOptions);
        } catch (IOException e) {
            e.printStackTrace();
        }

        assertTrue(fullyExecuted);
//        cluster.shutdown();
        socketPool.returnPort(queryPort);
        socketPool.returnPort(kafkaZkport);
        socketPool.returnPort(kafkaUnitport);
    }


    @Test
    public void testSimpleTopologyKeyRangeQuery() throws InterruptedException {
        DataSchema schema = new DataSchema();
        schema.addIntField("a1");
        schema.addDoubleField("a2");
        schema.addLongField("timestamp");
        schema.addVarcharField("a4", 100);
        schema.setPrimaryIndexField("a1");

        int ingestionPort = socketPool.getAvailablePort();
        int queryPort = socketPool.getAvailablePort();

        final int minIndex = 0;
        final int maxIndex = 100;

        TopologyGenerator<Integer> topologyGenerator = new TopologyGenerator<>();

        assertTrue(config != null);



        InputStreamReceiverBolt inputStreamReceiverBolt = new InputStreamReceiverBoltServer(schema, ingestionPort, config);
        QueryCoordinatorBolt<Integer> coordinator = new QueryCoordinatorWithQueryReceiverServerBolt<>(minIndex, maxIndex, queryPort,
                config, schema);

        StormTopology topology = topologyGenerator.generateIndexingTopology(schema, minIndex, maxIndex, false, inputStreamReceiverBolt,
                coordinator, config);

        Config conf = new Config();
        conf.setDebug(false);
        conf.setNumWorkers(1);

//        conf.put(Config.WORKER_CHILDOPTS, "-Xmx2048m");
//        conf.put(Config.WORKER_HEAP_MEMORY_MB, 2048);



        cluster.submitTopology("testSimpleTopologyKeyRangeQuery", conf, topology);

        final int tuples = 100000;


        final IngestionClientBatchMode ingestionClient = new IngestionClientBatchMode("localhost", ingestionPort, schema, 1024);
        try {
            ingestionClient.connectWithTimeout(50000);
        } catch (IOException e) {
            e.printStackTrace();
        }
        final QueryClient queryClient = new QueryClient("localhost", queryPort);
        try {
            queryClient.connectWithTimeout(50000);
        } catch (IOException e) {
            e.printStackTrace();
        }

        ExecutorService executorService = Executors.newCachedThreadPool();


        boolean fullyExecuted = false;

        for (int i = 0; i < tuples; i++) {
            DataTuple tuple = new DataTuple();
            tuple.add(i % 100);
            tuple.add(3.14);
            tuple.add(100L);
            tuple.add("payload");
            try {
                ingestionClient.appendInBatch(tuple);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        try {
            ingestionClient.flush();
        } catch (IOException e) {
            e.printStackTrace();
        }

        // wait for the tuples to be appended.
//        Thread.sleep(2000);
        ingestionClient.waitFinish();
        Thread.sleep(3000);

        try {

            // full key range query
            QueryResponse response = queryClient.query(new QueryRequest<>(minIndex, maxIndex, Long.MIN_VALUE, Long.MAX_VALUE));
            assertEquals(tuples, response.dataTuples.size());


            //half key range query
            response = queryClient.query(new QueryRequest<>(0, 49, Long.MIN_VALUE, Long.MAX_VALUE));
            assertEquals(tuples/2, response.dataTuples.size());

            //a key range query
            response =  queryClient.query(new QueryRequest<>(0,0, Long.MIN_VALUE, Long.MAX_VALUE));
            assertEquals(tuples/100, response.dataTuples.size());


            fullyExecuted = true;

        } catch (IOException e) {
            e.printStackTrace();
        }

        try {
            ingestionClient.close();
            queryClient.close();
            KillOptions killOptions = new KillOptions();
            killOptions.set_wait_secs(0);
            cluster.killTopologyWithOpts("testSimpleTopologyKeyRangeQuery", killOptions);
        } catch (IOException e) {
            e.printStackTrace();
        }

        assertTrue(fullyExecuted);
//        cluster.shutdown();
        socketPool.returnPort(ingestionPort);
        socketPool.returnPort(queryPort);
    }

    @Test
    public void testSimpleTopologyKeyRangeQueryOutOfBoundaries() throws InterruptedException {
        DataSchema schema = new DataSchema();
        schema.addIntField("a1");
        schema.addDoubleField("a2");
        schema.addLongField("timestamp");
        schema.addVarcharField("a4", 100);
        schema.setPrimaryIndexField("a1");

        final int minIndex = 20;
        final int maxIndex = 80;

        int ingestionPort = socketPool.getAvailablePort();
        int queryPort = socketPool.getAvailablePort();

        TopologyGenerator<Integer> topologyGenerator = new TopologyGenerator<>();

        InputStreamReceiverBolt inputStreamReceiverBolt = new InputStreamReceiverBoltServer(schema, ingestionPort, config);
        QueryCoordinatorBolt<Integer> coordinator = new QueryCoordinatorWithQueryReceiverServerBolt<>(minIndex, maxIndex, queryPort,
                config, schema);

        StormTopology topology = topologyGenerator.generateIndexingTopology(schema, minIndex, maxIndex, false, inputStreamReceiverBolt,
                coordinator, config);

        Config conf = new Config();
        conf.setDebug(false);
        conf.setNumWorkers(1);

//        conf.put(Config.WORKER_CHILDOPTS, "-Xmx2048m");
//        conf.put(Config.WORKER_HEAP_MEMORY_MB, 2048);


//        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology("testSimpleTopologyKeyRangeQueryOutOfBoundaries", conf, topology);

        final int tuples = 100000;


        final IngestionClientBatchMode ingestionClient = new IngestionClientBatchMode("localhost", ingestionPort, schema, 1024);
        try {
            ingestionClient.connectWithTimeout(50000);
        } catch (IOException e) {
            e.printStackTrace();
        }
        final QueryClient queryClient = new QueryClient("localhost", queryPort);
        try {
            queryClient.connectWithTimeout(50000);
        } catch (IOException e) {
            e.printStackTrace();
        }

        ExecutorService executorService = Executors.newCachedThreadPool();


        boolean fullyExecuted = false;

        for (int i = 0; i < tuples; i++) {
            DataTuple tuple = new DataTuple();
            tuple.add(i % 100);
            tuple.add(3.14);
            tuple.add(100L);
            tuple.add("payload");
            try {
                ingestionClient.appendInBatch(tuple);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        try {
            ingestionClient.flush();
        } catch (IOException e) {
            e.printStackTrace();
        }

        // wait for the tuples to be appended.
        ingestionClient.waitFinish();
        Thread.sleep(5000);

        try {

            // full key range query
            QueryResponse response = queryClient.query(new QueryRequest<>(0, 100, Long.MIN_VALUE, Long.MAX_VALUE));
            assertEquals(tuples, response.dataTuples.size());


            //half key range query
            response = queryClient.query(new QueryRequest<>(0, 49, Long.MIN_VALUE, Long.MAX_VALUE));
            assertEquals(tuples/2, response.dataTuples.size());

            //a key range query
            response =  queryClient.query(new QueryRequest<>(0,0, Long.MIN_VALUE, Long.MAX_VALUE));
            assertEquals(tuples/100, response.dataTuples.size());


            fullyExecuted = true;

        } catch (IOException e) {
            e.printStackTrace();
        }

        try {
            ingestionClient.close();
            queryClient.close();
            KillOptions killOptions = new KillOptions();
            killOptions.set_wait_secs(0);
            cluster.killTopologyWithOpts("testSimpleTopologyKeyRangeQueryOutOfBoundaries", killOptions);
        } catch (IOException e) {
            e.printStackTrace();
        }

        assertTrue(fullyExecuted);
//        cluster.shutdown();
        socketPool.returnPort(ingestionPort);
        socketPool.returnPort(queryPort);
    }

    @Test
    public void testSimpleTopologyPredicateWithBloomFilterVarcharTest() throws InterruptedException {
        DataSchema schema = new DataSchema();
        schema.addIntField("a1");
        schema.addDoubleField("a2");
        schema.addLongField("timestamp");
        schema.addVarcharField("a4", 100);
        schema.setPrimaryIndexField("a1");

        final int minIndex = 0;
        final int maxIndex = 100;

        int ingestionPort = socketPool.getAvailablePort();
        int queryPort = socketPool.getAvailablePort();

        TopologyGenerator<Integer> topologyGenerator = new TopologyGenerator<>();

        InputStreamReceiverBolt inputStreamReceiverBolt = new InputStreamReceiverBoltServer(schema, ingestionPort, config);
        QueryCoordinatorBolt<Integer> coordinator = new QueryCoordinatorWithQueryReceiverServerBolt<>(minIndex, maxIndex, queryPort,
                config, schema);

        ArrayList<String> bloomFilterColumns = new ArrayList<>();
        bloomFilterColumns.add("a4");
        StormTopology topology = topologyGenerator.generateIndexingTopology(schema, minIndex, maxIndex, false, inputStreamReceiverBolt,
                coordinator, null, bloomFilterColumns, config);

        Config conf = new Config();
        conf.setDebug(false);
        conf.setNumWorkers(1);

//        conf.put(Config.WORKER_CHILDOPTS, "-Xmx2048m");
//        conf.put(Config.WORKER_HEAP_MEMORY_MB, 2048);


//        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology("testSimpleTopologyPredicateWithBloomFilterVarcharTest", conf, topology);

        final int tuples = 100000;


        final IngestionClientBatchMode ingestionClient = new IngestionClientBatchMode("localhost", ingestionPort, schema, 1024);
        try {
            ingestionClient.connectWithTimeout(50000);
        } catch (IOException e) {
            e.printStackTrace();
        }
        final QueryClient queryClient = new QueryClient("localhost", queryPort);
        try {
            queryClient.connectWithTimeout(50000);
        } catch (IOException e) {
            e.printStackTrace();
        }

        ExecutorService executorService = Executors.newCachedThreadPool();


        boolean fullyExecuted = false;

        for (int i = 0; i < tuples; i++) {
            DataTuple tuple = new DataTuple();
            tuple.add(i % 100);
            tuple.add(3.14);
            tuple.add(100L);
            tuple.add("payload " + i % 100);
            try {
                ingestionClient.appendInBatch(tuple);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        try {
            ingestionClient.flush();
        } catch (IOException e) {
            e.printStackTrace();
        }

        // wait for the tuples to be appended.
        ingestionClient.waitFinish();
        Thread.sleep(5000);

        try {

            DataTuplePredicate predicate = t -> schema.getValue("a4", t).equals("payload 0");

//            DataTuplePredicate predicate = new DataTuplePredicate() {
//                @Override
//                public boolean test(DataTuple objects) {
//                    return (int)objects.get(0) == 0;
//                }
//            };

            // without equivalent hint
            {
                // full key range query
                QueryResponse response = queryClient.query(new QueryRequest<>(minIndex, maxIndex, Long.MIN_VALUE, Long.MAX_VALUE, predicate));
                assertEquals(tuples / 100, response.dataTuples.size());


                //half key range query
                response = queryClient.query(new QueryRequest<>(0, 49, Long.MIN_VALUE, Long.MAX_VALUE, predicate));
                assertEquals(tuples / 100, response.dataTuples.size());

                //a key range query
                response = queryClient.query(new QueryRequest<>(0, 0, Long.MIN_VALUE, Long.MAX_VALUE, predicate));
                assertEquals(tuples / 100, response.dataTuples.size());
            }

            // with equivalent hint
            {
                DataTupleEquivalentPredicateHint hint = new DataTupleEquivalentPredicateHint("a4", "payload 0");

                // full key range query
                QueryResponse response = queryClient.query(new QueryRequest<>(minIndex, maxIndex, Long.MIN_VALUE, Long.MAX_VALUE, predicate, null, null, null, hint));
                assertEquals(tuples / 100, response.dataTuples.size());


                //half key range query
                response = queryClient.query(new QueryRequest<>(0, 49, Long.MIN_VALUE, Long.MAX_VALUE, predicate, null, null, null, hint));
                assertEquals(tuples / 100, response.dataTuples.size());

                //a key range query
                response = queryClient.query(new QueryRequest<>(0, 0, Long.MIN_VALUE, Long.MAX_VALUE, predicate, null, null, null, hint));
                assertEquals(tuples / 100, response.dataTuples.size());
            }

            fullyExecuted = true;

        } catch (IOException e) {
            e.printStackTrace();
        }

        try {
            ingestionClient.close();
            queryClient.close();
            KillOptions killOptions = new KillOptions();
            killOptions.set_wait_secs(0);
            cluster.killTopologyWithOpts("testSimpleTopologyPredicateWithBloomFilterVarcharTest", killOptions);
        } catch (IOException e) {
            e.printStackTrace();
        }

        assertTrue(fullyExecuted);
//        cluster.shutdown();
        socketPool.returnPort(ingestionPort);
        socketPool.returnPort(queryPort);
    }



    @Test
    public void testSimpleTopologyPredicateWithBloomFilterDoubleTest() throws InterruptedException {
        DataSchema schema = new DataSchema();
        schema.addIntField("a1");
        schema.addDoubleField("a2");
        schema.addLongField("timestamp");
        schema.addVarcharField("a4", 100);
        schema.setPrimaryIndexField("a1");

        final int minIndex = 0;
        final int maxIndex = 100;

        int ingestionPort = socketPool.getAvailablePort();
        int queryPort = socketPool.getAvailablePort();

        TopologyGenerator<Integer> topologyGenerator = new TopologyGenerator<>();

        InputStreamReceiverBolt inputStreamReceiverBolt = new InputStreamReceiverBoltServer(schema, ingestionPort, config);
        QueryCoordinatorBolt<Integer> coordinator = new QueryCoordinatorWithQueryReceiverServerBolt<>(minIndex, maxIndex, queryPort,
                config, schema);

        ArrayList<String> bloomFilterColumns = new ArrayList<>();
        bloomFilterColumns.add("a2");
        StormTopology topology = topologyGenerator.generateIndexingTopology(schema, minIndex, maxIndex, false, inputStreamReceiverBolt,
                coordinator, null, bloomFilterColumns, config);

        Config conf = new Config();
        conf.setDebug(false);
        conf.setNumWorkers(1);

//        conf.put(Config.WORKER_CHILDOPTS, "-Xmx2048m");
//        conf.put(Config.WORKER_HEAP_MEMORY_MB, 2048);


//        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology("testSimpleTopologyPredicateWithBloomFilterDoubleTest", conf, topology);

        final int tuples = 100000;


        final IngestionClientBatchMode ingestionClient = new IngestionClientBatchMode("localhost", ingestionPort, schema, 1024);
        try {
            ingestionClient.connectWithTimeout(50000);
        } catch (IOException e) {
            e.printStackTrace();
        }
        final QueryClient queryClient = new QueryClient("localhost", queryPort);
        try {
            queryClient.connectWithTimeout(50000);
        } catch (IOException e) {
            e.printStackTrace();
        }

        ExecutorService executorService = Executors.newCachedThreadPool();


        boolean fullyExecuted = false;

        for (int i = 0; i < tuples; i++) {
            DataTuple tuple = new DataTuple();
            tuple.add(i % 100);
            tuple.add((double)(i % 100));
            tuple.add(100L);
            tuple.add("payload " + i % 100);
            try {
                ingestionClient.appendInBatch(tuple);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        try {
            ingestionClient.flush();
        } catch (IOException e) {
            e.printStackTrace();
        }

        // wait for the tuples to be appended.
        ingestionClient.waitFinish();
        Thread.sleep(3000);

        try {

            DataTuplePredicate predicate = t -> schema.getValue("a2", t).equals(0.0);

//            DataTuplePredicate predicate = new DataTuplePredicate() {
//                @Override
//                public boolean test(DataTuple objects) {
//                    return (int)objects.get(0) == 0;
//                }
//            };

            // without equivalent hint
            {
                // full key range query
                QueryResponse response = queryClient.query(new QueryRequest<>(minIndex, maxIndex, Long.MIN_VALUE, Long.MAX_VALUE, predicate));
                assertTrue(response.getSchema() != null);
                assertEquals(tuples / 100, response.dataTuples.size());


                //half key range query
                response = queryClient.query(new QueryRequest<>(0, 49, Long.MIN_VALUE, Long.MAX_VALUE, predicate));
                assertTrue(response.getSchema() != null);
                assertEquals(tuples / 100, response.dataTuples.size());

                //a key range query
                response = queryClient.query(new QueryRequest<>(0, 0, Long.MIN_VALUE, Long.MAX_VALUE, predicate));
                assertTrue(response.getSchema() != null);
                assertEquals(tuples / 100, response.dataTuples.size());
            }

            // with equivalent hint
            {
                DataTupleEquivalentPredicateHint hint = new DataTupleEquivalentPredicateHint("a2", 0.0);

                // full key range query
                QueryResponse response = queryClient.query(new QueryRequest<>(minIndex, maxIndex, Long.MIN_VALUE, Long.MAX_VALUE, predicate, null, null, null, hint));
                assertEquals(tuples / 100, response.dataTuples.size());


                //half key range query
                response = queryClient.query(new QueryRequest<>(0, 49, Long.MIN_VALUE, Long.MAX_VALUE, predicate, null, null, null, hint));
                assertEquals(tuples / 100, response.dataTuples.size());

                //a key range query
                response = queryClient.query(new QueryRequest<>(0, 0, Long.MIN_VALUE, Long.MAX_VALUE, predicate, null, null, null, hint));
                assertEquals(tuples / 100, response.dataTuples.size());
            }

            fullyExecuted = true;

        } catch (IOException e) {
            e.printStackTrace();
        }

        try {
            ingestionClient.close();
            queryClient.close();
            KillOptions killOptions = new KillOptions();
            killOptions.set_wait_secs(0);
            cluster.killTopologyWithOpts("testSimpleTopologyPredicateWithBloomFilterDoubleTest", killOptions);
        } catch (IOException e) {
            e.printStackTrace();
        }

        assertTrue(fullyExecuted);
//        cluster.shutdown();
        socketPool.returnPort(ingestionPort);
        socketPool.returnPort(queryPort);
    }

    @Test
    public void testSimpleTopologyAggregation() throws InterruptedException {
        DataSchema schema = new DataSchema();
        schema.addIntField("a1");
        schema.addDoubleField("a2");
        schema.addLongField("timestamp");
        schema.addVarcharField("a4", 100);
        schema.setPrimaryIndexField("a1");

        final int minIndex = 0;
        final int maxIndex = 100;

        int ingestionPort = socketPool.getAvailablePort();
        int queryPort = socketPool.getAvailablePort();

        assertTrue(config != null);

        TopologyGenerator<Integer> topologyGenerator = new TopologyGenerator<>();

        InputStreamReceiverBolt inputStreamReceiverBolt = new InputStreamReceiverBoltServer(schema, ingestionPort, config);
        QueryCoordinatorBolt<Integer> coordinator = new QueryCoordinatorWithQueryReceiverServerBolt<>(minIndex, maxIndex, queryPort,
                config, schema);

        StormTopology topology = topologyGenerator.generateIndexingTopology(schema, minIndex, maxIndex, false, inputStreamReceiverBolt,
                coordinator, config);

        Config conf = new Config();
        conf.setDebug(false);
        conf.setNumWorkers(1);

//        conf.put(Config.WORKER_CHILDOPTS, "-Xmx2048m");
//        conf.put(Config.WORKER_HEAP_MEMORY_MB, 2048);


//        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology("testSimpleTopologyAggregation", conf, topology);

        final int tuples = 100000;


        final IngestionClientBatchMode ingestionClient = new IngestionClientBatchMode("localhost", ingestionPort, schema, 1024);
        try {
            ingestionClient.connectWithTimeout(50000);
        } catch (IOException e) {
            e.printStackTrace();
        }
        final QueryClient queryClient = new QueryClient("localhost", queryPort);
        try {
            queryClient.connectWithTimeout(50000);
        } catch (IOException e) {
            e.printStackTrace();
        }

        ExecutorService executorService = Executors.newCachedThreadPool();


        boolean fullyExecuted = false;

        for (int i = 0; i < tuples; i++) {
            DataTuple tuple = new DataTuple();
            tuple.add(i / (tuples / 100));
            tuple.add((double)(i % 1000));
            tuple.add(100L);
            tuple.add("payload");
            try {
                ingestionClient.appendInBatch(tuple);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        try {
            ingestionClient.flush();
        } catch (IOException e) {
            e.printStackTrace();
        }

        // wait for the tuples to be appended.
        ingestionClient.waitFinish();
        Thread.sleep(3000);

        try {

            Aggregator<Integer> aggregator = new Aggregator<>(schema, "a1", new AggregateField(new Count(), "*")
                    , new AggregateField(new Min<>(), "a2"), new AggregateField(new Max<>(), "a2"));

            DataTuplePredicate postPredicate = t -> (int)aggregator.getOutputDataSchema().getValue("a1", t) < 100000;

            // full key range query
            QueryResponse response = queryClient.query(new QueryRequest<>(minIndex, maxIndex, Long.MIN_VALUE,
                    Long.MAX_VALUE, null, postPredicate, aggregator));
            assertTrue(response.getSchema() != null);
            assertEquals(100, response.dataTuples.size());
            for (DataTuple tuple: response.dataTuples) {
                assertEquals((double)tuples/100, (double)tuple.get(1), 0.0001);
                assertEquals(0.0, (double)tuple.get(2), 0.0001);
                assertEquals(999.0, (double)tuple.get(3), 0.0001);
            }

            //half key range query
            response = queryClient.query(new QueryRequest<>(0, 49, Long.MIN_VALUE, Long.MAX_VALUE, aggregator));
            assertTrue(response.getSchema() != null);
            assertEquals(50, response.dataTuples.size());
            for (DataTuple tuple: response.dataTuples) {
                assertEquals((double)tuples/100, (double)tuple.get(1), 0.0001);
                assertEquals(0.0, (double)tuple.get(2), 0.0001);
                assertEquals(999.0, (double)tuple.get(3), 0.0001);
            }

            //a key range query
            response =  queryClient.query(new QueryRequest<>(0,0, Long.MIN_VALUE, Long.MAX_VALUE, aggregator));
            assertTrue(response.getSchema() != null);
            assertEquals(1, response.dataTuples.size());
            for (DataTuple tuple: response.dataTuples) {
                assertEquals((double)tuples/100, (double)tuple.get(1), 0.0001);
                assertEquals(0.0, (double)tuple.get(2), 0.0001);
                assertEquals(999.0, (double)tuple.get(3), 0.0001);
            }


            fullyExecuted = true;

        } catch (IOException e) {
            e.printStackTrace();
        }

        try {
            ingestionClient.close();
            queryClient.close();
            KillOptions killOptions = new KillOptions();
            killOptions.set_wait_secs(0);
            cluster.killTopologyWithOpts("testSimpleTopologyAggregation", killOptions);
        } catch (IOException e) {
            e.printStackTrace();
        }

        assertTrue(fullyExecuted);
//        cluster.shutdown();
        socketPool.returnPort(ingestionPort);
        socketPool.returnPort(queryPort);
    }

    @Test
    public void testSimpleTopologyMapperAggregation() throws InterruptedException {
        DataSchema rawSchema = new DataSchema();
        rawSchema.addIntField("a1");
        rawSchema.addDoubleField("a2");
        rawSchema.addLongField("timestamp");
        rawSchema.addVarcharField("a4", 100);
        rawSchema.setPrimaryIndexField("a1");

        DataSchema schema = rawSchema.duplicate();
        schema.addDoubleField("a3");

        DataTupleMapper mapper = new DataTupleMapper(rawSchema, (Serializable & Function<DataTuple, DataTuple>) (DataTuple t) -> {
            t.add((double)t.get(1) * 2);
            return t;});

        final int minIndex = 0;
        final int maxIndex = 100;

        int ingestionPort = socketPool.getAvailablePort();
        int queryPort = socketPool.getAvailablePort();

        TopologyGenerator<Integer> topologyGenerator = new TopologyGenerator<>();

        InputStreamReceiverBolt inputStreamReceiverBolt = new InputStreamReceiverBoltServer(rawSchema, ingestionPort, config);
        QueryCoordinatorBolt<Integer> coordinator = new QueryCoordinatorWithQueryReceiverServerBolt<>(minIndex, maxIndex, queryPort,
                config, schema);

        StormTopology topology = topologyGenerator.generateIndexingTopology(schema, minIndex, maxIndex, false, inputStreamReceiverBolt,
                coordinator, mapper, config);

        Config conf = new Config();
        conf.setDebug(false);
        conf.setNumWorkers(1);

//        conf.put(Config.WORKER_CHILDOPTS, "-Xmx2048m");
//        conf.put(Config.WORKER_HEAP_MEMORY_MB, 2048);

        cluster.submitTopology("testSimpleTopologyMapperAggregation", conf, topology);
//        LocalCluster cluster = new LocalCluster();
        final int tuples = 100000;


        final IngestionClientBatchMode ingestionClient = new IngestionClientBatchMode("localhost", ingestionPort, rawSchema, 1024);
        try {
            ingestionClient.connectWithTimeout(50000);
        } catch (IOException e) {
            e.printStackTrace();
        }
        final QueryClient queryClient = new QueryClient("localhost", queryPort);
        try {
            queryClient.connectWithTimeout(50000);
        } catch (IOException e) {
            e.printStackTrace();
        }

        ExecutorService executorService = Executors.newCachedThreadPool();


        boolean fullyExecuted = false;

        for (int i = 0; i < tuples; i++) {
            DataTuple tuple = new DataTuple();
            tuple.add(i / (tuples / 100));
            tuple.add((double)(i % 1000));
            tuple.add(100L);
            tuple.add("payload");
            try {
                ingestionClient.appendInBatch(tuple);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        try {
            ingestionClient.flush();
        } catch (IOException e) {
            e.printStackTrace();
        }

        // wait for the tuples to be appended.
        ingestionClient.waitFinish();
        Thread.sleep(3000);

        try {

            Aggregator<Integer> aggregator = new Aggregator<>(schema, "a1", new AggregateField(new Count(), "*")
                    , new AggregateField(new Min<>(), "a3"), new AggregateField(new Max<>(), "a3"));

            // full key range query
            QueryResponse response = queryClient.query(new QueryRequest<>(minIndex, maxIndex, Long.MIN_VALUE,
                    Long.MAX_VALUE, aggregator));
            assertTrue(response.getSchema() != null);
            assertEquals(100, response.dataTuples.size());
            for (DataTuple tuple: response.dataTuples) {
                assertEquals((double)tuples/100, (double)tuple.get(1), 0.0001);
                assertEquals(0.0, (double)tuple.get(2), 0.0001);
                assertEquals(999.0 * 2, (double)tuple.get(3), 0.0001);
            }

            //half key range query
            response = queryClient.query(new QueryRequest<>(0, 49, Long.MIN_VALUE, Long.MAX_VALUE, aggregator));
            assertTrue(response.getSchema() != null);
            assertEquals(50, response.dataTuples.size());
            for (DataTuple tuple: response.dataTuples) {
                assertEquals((double)tuples/100, (double)tuple.get(1), 0.0001);
                assertEquals(0.0, (double)tuple.get(2), 0.0001);
                assertEquals(999.0 * 2, (double)tuple.get(3), 0.0001);
            }

            //a key range query
            response =  queryClient.query(new QueryRequest<>(0,0, Long.MIN_VALUE, Long.MAX_VALUE, aggregator));
            assertTrue(response.getSchema() != null);
            assertEquals(1, response.dataTuples.size());
            for (DataTuple tuple: response.dataTuples) {
                assertEquals((double)tuples/100, (double)tuple.get(1), 0.0001);
                assertEquals(0.0, (double)tuple.get(2), 0.0001);
                assertEquals(999.0 * 2, (double)tuple.get(3), 0.0001);
            }


            fullyExecuted = true;

        } catch (IOException e) {
            e.printStackTrace();
        }

        try {
            ingestionClient.close();
            queryClient.close();
            KillOptions killOptions = new KillOptions();
            killOptions.set_wait_secs(0);
            cluster.killTopologyWithOpts("testSimpleTopologyMapperAggregation", killOptions);
        } catch (IOException e) {
            e.printStackTrace();
        }

        assertTrue(fullyExecuted);
//        cluster.shutdown();
        socketPool.returnPort(ingestionPort);
        socketPool.returnPort(queryPort);
    }

    @Test
    public void testSimpleTopologySort() throws InterruptedException {
        DataSchema schema = new DataSchema();
        schema.addIntField("a1");
        schema.addDoubleField("a2");
        schema.addLongField("timestamp");
        schema.addVarcharField("a4", 100);
        schema.setPrimaryIndexField("a1");

        final int minIndex = 0;
        final int maxIndex = 1000;

        int ingestionPort = socketPool.getAvailablePort();
        int queryPort = socketPool.getAvailablePort();

        TopologyGenerator<Integer> topologyGenerator = new TopologyGenerator<>();

        InputStreamReceiverBolt inputStreamReceiverBolt = new InputStreamReceiverBoltServer(schema, ingestionPort, config);
        QueryCoordinatorBolt<Integer> coordinator = new QueryCoordinatorWithQueryReceiverServerBolt<>(minIndex, maxIndex, queryPort,
                config, schema);

        StormTopology topology = topologyGenerator.generateIndexingTopology(schema, minIndex, maxIndex, false, inputStreamReceiverBolt,
                coordinator, config);

        Config conf = new Config();
        conf.setDebug(false);
        conf.setNumWorkers(1);

//        conf.put(Config.WORKER_CHILDOPTS, "-Xmx2048m");
//        conf.put(Config.WORKER_HEAP_MEMORY_MB, 2048);


//        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology("testSimpleTopologySort", conf, topology);

        final int tuples = 100000;


        final IngestionClientBatchMode ingestionClient = new IngestionClientBatchMode("localhost", ingestionPort, schema, 1024);
        try {
            ingestionClient.connectWithTimeout(50000);
        } catch (IOException e) {
            e.printStackTrace();
        }
        final QueryClient queryClient = new QueryClient("localhost", queryPort);
        try {
            queryClient.connectWithTimeout(50000);
        } catch (IOException e) {
            e.printStackTrace();
        }

        ExecutorService executorService = Executors.newCachedThreadPool();


        boolean fullyExecuted = false;

        for (int i = 0; i < tuples; i++) {
            DataTuple tuple = new DataTuple();
            tuple.add(i / 1000);
            tuple.add((double)(i % 1000));
            tuple.add(100L);
            tuple.add("payload");
            try {
                ingestionClient.appendInBatch(tuple);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        try {
            ingestionClient.flush();
        } catch (IOException e) {
            e.printStackTrace();
        }

        // wait for the tuples to be appended.
        ingestionClient.waitFinish();
        Thread.sleep(3000);

        try {

//            Aggregator<Integer> aggregator = new Aggregator<>(schema, "a1", new AggregateField(new Count(), "*")
//                    , new AggregateField(new Min<>(), "a2"), new AggregateField(new Max<>(), "a2"));

            DataTuplePredicate predicate = t -> (int)schema.getValue("a1", t) < 20;

            DataTupleSorter sorter = (x, y) -> Integer.compare((int)schema.getValue("a1", x),
                    (int)schema.getValue("a1", y));

            // full key range query
            QueryResponse response = queryClient.query(new QueryRequest<>(minIndex, maxIndex, Long.MIN_VALUE,
                    Long.MAX_VALUE, predicate, null, null, sorter));
            for (int i = 1; i < response.dataTuples.size(); i++) {
                assertTrue((int)response.dataTuples.get(i -1).get(0)<=(int)response.dataTuples.get(i).get(0));
            }

            //half key range query
            response = queryClient.query(new QueryRequest<>(0, 49, Long.MIN_VALUE, Long.MAX_VALUE, predicate,null, null, sorter));
            for (int i = 1; i < response.dataTuples.size(); i++) {
                assertTrue((int)response.dataTuples.get(i -1).get(0)<=(int)response.dataTuples.get(i).get(0));
            }

            //a key range query
            response =  queryClient.query(new QueryRequest<>(0,0, Long.MIN_VALUE, Long.MAX_VALUE, predicate,null, null, sorter));
            for (int i = 1; i < response.dataTuples.size(); i++) {
                assertTrue((int)response.dataTuples.get(i -1).get(0)<=(int)response.dataTuples.get(i).get(0));
            }


            fullyExecuted = true;

        } catch (IOException e) {
            e.printStackTrace();
        }

        try {
            ingestionClient.close();
            queryClient.close();
            KillOptions killOptions = new KillOptions();
            killOptions.set_wait_secs(0);
            cluster.killTopologyWithOpts("testSimpleTopologySort", killOptions);
        } catch (IOException e) {
            e.printStackTrace();
        }

        assertTrue(fullyExecuted);
//        cluster.shutdown();
        socketPool.returnPort(ingestionPort);
        socketPool.returnPort(queryPort);
    }

    @Test
    public void testSimpleTopologyTemporalQuery() throws InterruptedException {
        DataSchema schema = new DataSchema();
        schema.addIntField("a1");
        schema.addDoubleField("a2");
        schema.addLongField("timestamp");
        schema.addVarcharField("a4", 100);
        schema.setPrimaryIndexField("a1");

        final int minIndex = 0;
        final int maxIndex = 1000;

        int ingestionPort = socketPool.getAvailablePort();
        int queryPort = socketPool.getAvailablePort();

        TopologyGenerator<Integer> topologyGenerator = new TopologyGenerator<>();

        InputStreamReceiverBolt inputStreamReceiverBolt = new InputStreamReceiverBoltServer(schema, ingestionPort, config);
        QueryCoordinatorBolt<Integer> coordinator = new QueryCoordinatorWithQueryReceiverServerBolt<>(minIndex, maxIndex, queryPort,
                config, schema);

        StormTopology topology = topologyGenerator.generateIndexingTopology(schema, minIndex, maxIndex, false, inputStreamReceiverBolt,
                coordinator, config);

        Config conf = new Config();
        conf.setDebug(false);
        conf.setNumWorkers(1);

//        conf.put(Config.WORKER_CHILDOPTS, "-Xmx2048m");
//        conf.put(Config.WORKER_HEAP_MEMORY_MB, 2048);


//        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology("testSimpleTopologyTemporalQuery", conf, topology);

        final int tuples = 100000;


        final IngestionClientBatchMode ingestionClient = new IngestionClientBatchMode("localhost", ingestionPort, schema, 1024);
        try {
            ingestionClient.connectWithTimeout(50000);
        } catch (IOException e) {
            e.printStackTrace();
        }
        final QueryClient queryClient = new QueryClient("localhost", queryPort);
        try {
            queryClient.connectWithTimeout(50000);
        } catch (IOException e) {
            e.printStackTrace();
        }

        ExecutorService executorService = Executors.newCachedThreadPool();


        boolean fullyExecuted = false;

        for (int i = 0; i < tuples; i++) {
            DataTuple tuple = new DataTuple();
            tuple.add(i % 100);
            tuple.add((double)(i % 100));
            tuple.add((long)(i / (tuples / 100)));
            tuple.add("payload");
            try {
                ingestionClient.appendInBatch(tuple);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        try {
            ingestionClient.flush();
        } catch (IOException e) {
            e.printStackTrace();
        }

        // wait for the tuples to be appended.
        ingestionClient.waitFinish();
        Thread.sleep(3000);

        try {

            Aggregator<Integer> aggregator = new Aggregator<>(schema, null,
                    new AggregateField(new Count(), "*"));


            // full temporal range query
            QueryResponse response = queryClient.query(new QueryRequest<>(0, 1000, Long.MIN_VALUE,
                    Long.MAX_VALUE, aggregator));
            assertEquals((double)tuples, response.dataTuples.get(0).get(0));

            //half temporal range query
            response = queryClient.query(new QueryRequest<>(0, 100, 0, 49, aggregator));
            assertEquals((double)tuples / 2, response.dataTuples.get(0).get(0));

            //a temporal range query
            response =  queryClient.query(new QueryRequest<>(0,100, 0, 0, aggregator));
            assertEquals((double)tuples / 100, response.dataTuples.get(0).get(0));


            fullyExecuted = true;

        } catch (IOException e) {
            e.printStackTrace();
        }

        try {
            ingestionClient.close();
            queryClient.close();
            KillOptions killOptions = new KillOptions();
            killOptions.set_wait_secs(0);
            cluster.killTopologyWithOpts("testSimpleTopologyTemporalQuery", killOptions);
        } catch (IOException e) {
            e.printStackTrace();
        }

        assertTrue(fullyExecuted);
        socketPool.returnPort(ingestionPort);
        socketPool.returnPort(queryPort);
    }

    @Test
    public void testSchemaQuery() throws InterruptedException, IOException, ClassNotFoundException {
        DataSchema schema = new DataSchema();
        schema.addIntField("a1");
        schema.addDoubleField("a2");
        schema.addLongField("timestamp");
        schema.addVarcharField("a4", 100);
        schema.setPrimaryIndexField("a1");

        final int minIndex = 0;
        final int maxIndex = 1000;

        int ingestionPort = socketPool.getAvailablePort();
        int queryPort = socketPool.getAvailablePort();

        TopologyGenerator<Integer> topologyGenerator = new TopologyGenerator<>();

        InputStreamReceiverBolt inputStreamReceiverBolt = new InputStreamReceiverBoltServer(schema, ingestionPort, config);
        QueryCoordinatorBolt<Integer> coordinator = new QueryCoordinatorWithQueryReceiverServerBolt<>(minIndex, maxIndex, queryPort,
                config, schema);

        StormTopology topology = topologyGenerator.generateIndexingTopology(schema, minIndex, maxIndex, false, inputStreamReceiverBolt,
                coordinator, config);

        Config conf = new Config();
        conf.setDebug(false);
        conf.setNumWorkers(1);

//        conf.put(Config.WORKER_CHILDOPTS, "-Xmx2048m");
//        conf.put(Config.WORKER_HEAP_MEMORY_MB, 2048);


//        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology("testSimpleTopologyTemporalQuery", conf, topology);

        QueryClient client = new QueryClient("localhost", queryPort);
        client.connectWithTimeout(50000);
        DataSchema querySchema = client.querySchema();

        assertEquals(schema.toString(), querySchema.toString());



        KillOptions killOptions = new KillOptions();
        killOptions.set_wait_secs(0);
        cluster.killTopologyWithOpts("testSimpleTopologyTemporalQuery", killOptions);

        socketPool.returnPort(ingestionPort);
        socketPool.returnPort(queryPort);
    }

    @Test
    public void testSimpleTopologyPostPredicate() throws InterruptedException {
        DataSchema schema = new DataSchema();
        schema.addIntField("a1");
        schema.addDoubleField("a2");
        schema.addLongField("timestamp");
        schema.addVarcharField("a4", 100);
        schema.setPrimaryIndexField("a1");

        final int minIndex = 0;
        final int maxIndex = 100;

        int ingestionPort = socketPool.getAvailablePort();
        int queryPort = socketPool.getAvailablePort();

        assertTrue(config != null);

        TopologyGenerator<Integer> topologyGenerator = new TopologyGenerator<>();

        InputStreamReceiverBolt inputStreamReceiverBolt = new InputStreamReceiverBoltServer(schema, ingestionPort, config);
        QueryCoordinatorBolt<Integer> coordinator = new QueryCoordinatorWithQueryReceiverServerBolt<>(minIndex, maxIndex, queryPort,
                config, schema);

        StormTopology topology = topologyGenerator.generateIndexingTopology(schema, minIndex, maxIndex, false, inputStreamReceiverBolt,
                coordinator, config);

        Config conf = new Config();
        conf.setDebug(false);
        conf.setNumWorkers(1);

//        conf.put(Config.WORKER_CHILDOPTS, "-Xmx2048m");
//        conf.put(Config.WORKER_HEAP_MEMORY_MB, 2048);


//        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology("testSimpleTopologyPostPredicate", conf, topology);

        final int tuples = 100000;


        final IngestionClientBatchMode ingestionClient = new IngestionClientBatchMode("localhost", ingestionPort, schema, 1024);
        try {
            ingestionClient.connectWithTimeout(50000);
        } catch (IOException e) {
            e.printStackTrace();
        }
        final QueryClient queryClient = new QueryClient("localhost", queryPort);
        try {
            queryClient.connectWithTimeout(50000);
        } catch (IOException e) {
            e.printStackTrace();
        }

        ExecutorService executorService = Executors.newCachedThreadPool();


        boolean fullyExecuted = false;

        for (int i = 0; i < tuples; i++) {
            DataTuple tuple = new DataTuple();
            tuple.add(i / (tuples / 100));
            tuple.add((double)(i % 1000));
            tuple.add(100L);
            tuple.add("payload");
            try {
                ingestionClient.appendInBatch(tuple);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        try {
            ingestionClient.flush();
        } catch (IOException e) {
            e.printStackTrace();
        }

        // wait for the tuples to be appended.
        ingestionClient.waitFinish();
        Thread.sleep(3000);

        try {

            Aggregator<Integer> aggregator = new Aggregator<>(schema, "a1", new AggregateField(new Count(), "*")
                    , new AggregateField(new Min<>(), "a2"), new AggregateField(new Max<>(), "a2"));

            DataTuplePredicate postPredicate = t -> Double.parseDouble(aggregator.getOutputDataSchema().getValue("max(a2)", t).toString()) < 1000.0;

            // full key range query
            QueryResponse response = queryClient.query(new QueryRequest<>(minIndex, maxIndex, Long.MIN_VALUE,
                    Long.MAX_VALUE, null, postPredicate, aggregator));
            assertTrue(response.getSchema() != null);
//            System.out.println("dddddddddddd" + response.getTuples().get(0).get(3));
            assertEquals(100, response.dataTuples.size());

            fullyExecuted = true;

        } catch (IOException e) {
            e.printStackTrace();
        }

        try {
            ingestionClient.close();
            queryClient.close();
            KillOptions killOptions = new KillOptions();
            killOptions.set_wait_secs(0);
            cluster.killTopologyWithOpts("testSimpleTopologyPostPredicate", killOptions);
        } catch (IOException e) {
            e.printStackTrace();
        }

        assertTrue(fullyExecuted);
//        cluster.shutdown();
        socketPool.returnPort(ingestionPort);
        socketPool.returnPort(queryPort);
    }

}
