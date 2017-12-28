package indexingTopology.topology;

import indexingTopology.api.client.GeoTemporalQueryClient;
import indexingTopology.api.client.GeoTemporalQueryRequest;
import indexingTopology.api.client.IngestionKafkaBatchMode;
import indexingTopology.api.client.QueryResponse;
import indexingTopology.bolt.GeoTemporalQueryCoordinatorBoltBolt;
import indexingTopology.bolt.KafkaReceiverBoltTest;
import indexingTopology.bolt.QueryCoordinatorBolt;
import indexingTopology.common.data.DataSchema;
import indexingTopology.common.data.DataTuple;
import indexingTopology.common.logics.DataTupleMapper;
import indexingTopology.common.logics.DataTuplePredicate;
import indexingTopology.config.TopologyConfig;
import indexingTopology.util.AvailableSocketPool;
import indexingTopology.util.shape.Point;
import indexingTopology.util.shape.Rectangle;
import indexingTopology.util.taxi.Car;
import indexingTopology.util.taxi.City;
import indexingTopology.util.taxi.TrajectoryGenerator;
import indexingTopology.util.taxi.TrajectoryMovingGenerator;
import junit.framework.TestCase;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.generated.KillOptions;
import org.apache.storm.generated.StormTopology;
import org.junit.Test;

import java.io.IOException;
import java.io.Serializable;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.function.Function;

import static junit.framework.TestCase.assertEquals;
import static junit.framework.TestCase.assertTrue;

/**
 * Created by billlin on 2017/12/27
 */
public class KafkaTopologyTest extends TestCase{
    TopologyConfig config = new TopologyConfig();

    AvailableSocketPool socketPool = new AvailableSocketPool();

    LocalCluster cluster;

    Producer<String, String> producer = null;
    int totalNumber = 0;
    int meetRequirements = 0;

    boolean setupDone = false;

    boolean tearDownDone = false;

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
        rawSchema.addLongField("locationtime");
        rawSchema.setTemporalField("locationtime");

        DataSchema schema = new DataSchema();
        schema.addDoubleField("lon");
        schema.addDoubleField("lat");
        schema.addIntField("devbtype");
        schema.addVarcharField("devid", 8);
        schema.addVarcharField("id", 32);
        schema.addLongField("locationtime");
        schema.setTemporalField("locationtime");
        schema.addIntField("zcode");
        schema.setPrimaryIndexField("zcode");

        int queryPort = socketPool.getAvailablePort();
        int kafkaZkport = socketPool.getAvailablePort();
        int kafkaUnitport = socketPool.getAvailablePort();

        System.out.println("kafkaZkport: " + kafkaZkport + " " + kafkaUnitport);

        Thread.sleep(1000);
        double x1 = 80.0;
        double x2 = 90.0;
        double y1 = 70.0;
        double y2 = 80.0;
        int partitions = 128;
        City city = new City(x1, x2, y1, y2, partitions);
        Integer lowerBound = 0;
        Integer upperBound = 5000;

        final int minIndex = 0;
        final int maxIndex = 99999;

        TopologyGenerator<Integer> topologyGenerator = new TopologyGenerator<>();

        String confFile = "../conf/confTest.yaml";
        config.override(confFile);
        System.out.println("Topology is overridden by " + confFile);
        System.out.println(config.getCriticalSettings());
        assertTrue(config != null);
        config.ZKHost.clear();
        config.ZKHost.add("localhost:" + String.valueOf(kafkaZkport));
        config.kafkaHost.clear();
        config.kafkaHost.add("localhost:" + String.valueOf(kafkaUnitport));

        int total = 100;
        long start = System.currentTimeMillis();
        HashMap matchDataTuple = new HashMap();
        System.out.println("Kafka Producer send msg start,total msgs:"+total);



//        FakeKafkaReceiverBolt inputStreamReceiverBolt = new FakeKafkaReceiverBolt(rawSchema, config, kafkaUnitServer.getKafkaConnect(), "consumer",total);
        KafkaReceiverBoltTest inputStreamReceiverBolt = new KafkaReceiverBoltTest(rawSchema, config);

        QueryCoordinatorBolt<Integer> coordinator = new GeoTemporalQueryCoordinatorBoltBolt<>(lowerBound,
                upperBound, queryPort, city, config, schema);

        DataTupleMapper dataTupleMapper = new DataTupleMapper(rawSchema, (Serializable & Function<DataTuple, DataTuple>) t -> {
            double lon = (double)schema.getValue("lon", t);
            double lat = (double)schema.getValue("lat", t);
            int zcode = city.getZCodeForALocation(lon, lat);
            t.add(zcode);
            return t;
        });



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

        cluster.submitTopology("testKafkaTopologyKeyRangeQuery", conf, topology);

//        Thread emittingThread;
//        emittingThread = new Thread(() -> {
        try {
            Thread.sleep(20000);// wait for kafka start
            System.out.println("here");
            String currentKafkahost = config.kafkaHost.get(0);
            Properties props = new Properties();
            props.put("group.id", 0);
            props.put("key.serializer", StringSerializer.class.getName());
            props.put("value.serializer", StringSerializer.class.getName());
            props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, currentKafkahost);
            Producer<String, String> producer = new KafkaProducer<>(props);
            IngestionKafkaBatchMode kafkaBatchMode = new IngestionKafkaBatchMode(currentKafkahost, config.topic);
            kafkaBatchMode.ingestProducer();
            TrajectoryGenerator generator = new TrajectoryMovingGenerator(x1, x2, y1, y2, 100000, 45.0);
//            while (!Thread.currentThread().isInterrupted()) {
            for (int i = 0; i < total; i++) {
                DataTuple dataTuple = new DataTuple();
                Car car = generator.generate();
                totalNumber++;
                Long timestamp = System.currentTimeMillis();
                String locationtime = String.valueOf(new Date(timestamp));
                int devbtype = (int) (Math.random() * 10);
                Double lon = Math.random() * 100;
                Double lat = Math.random() * 100;
                String idString = "" + i;
                dataTuple.add(lon);
                dataTuple.add(lat);
                dataTuple.add(devbtype);
                dataTuple.add("asd");
                dataTuple.add(idString);
                dataTuple.add(System.currentTimeMillis());
                if(new Rectangle(new Point(x1,y2),new Point(x2,y1)).checkIn(new Point(lon,lat))){
                    meetRequirements++;
                    matchDataTuple.put(idString,dataTuple);
                }
//                        if (lon >= x1 && lon <= x2 && lat >= y1 && lat <= y2) {
//                            meetRequirements++;
//                            matchDataTuple.put(idString,dataTuple);
//                        }
                String Msg = "{\"lon\":" + lon + ",\"lat\":" + lat + ",\"devbtype\":" + devbtype + ",\"devid\":\"asd\",\"id\":" + idString + ",\"locationtime\":" + System.currentTimeMillis() + "}";
                kafkaBatchMode.send(i, Msg);
            }
            kafkaBatchMode.flush();
            System.out.println("Kafka Producer send msg over,cost time:" + (System.currentTimeMillis() - start) + "ms");
            kafkaBatchMode.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
//        });
//        emittingThread.start();
        GeoTemporalQueryClient queryClient = new GeoTemporalQueryClient("localhost", queryPort);

        try {
            queryClient.connectWithTimeout(10000);
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
            for(int i = 0; i < response.dataTuples.size(); i++){
                String idString = "" + response.dataTuples.get(i).get(4);
                DataTuple dataTuple = (DataTuple)matchDataTuple.get(idString);
                response.dataTuples.get(i).remove(6);
                assertEquals(dataTuple,response.dataTuples.get(i));
            }
            System.out.println(queryPort);
            System.out.println("meetRequirements:" + meetRequirements);
            System.out.println("dataTuples.size:" + response.dataTuples.size());
            assertEquals(meetRequirements, response.dataTuples.size());
            fullyExecuted = true;

        } catch (IOException e) {
            e.printStackTrace();
        }

        try {
            queryClient.close();
            KillOptions killOptions = new KillOptions();
            killOptions.set_wait_secs(0);
            cluster.killTopologyWithOpts("testKafkaTopologyKeyRangeQuery", killOptions);
        } catch (IOException e) {
            e.printStackTrace();
        }

        assertTrue(fullyExecuted);
//        cluster.shutdown();
        System.out.println("asdasdasd: "+queryPort+" "+kafkaZkport+" "+kafkaUnitport);
        socketPool.returnPort(queryPort);
        socketPool.returnPort(kafkaZkport);
        socketPool.returnPort(kafkaUnitport);
    }
}
