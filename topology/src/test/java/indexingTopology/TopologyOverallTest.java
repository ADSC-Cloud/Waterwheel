package indexingTopology;

import indexingTopology.common.aggregator.AggregateField;
import indexingTopology.common.aggregator.Aggregator;
import indexingTopology.common.aggregator.Count;
import indexingTopology.common.aggregator.Sum;
import indexingTopology.api.client.IngestionClient;
import indexingTopology.api.client.QueryClient;
import indexingTopology.api.client.QueryRequest;
import indexingTopology.api.client.QueryResponse;
import indexingTopology.bolt.InputStreamReceiverBolt;
import indexingTopology.bolt.InputStreamReceiverBoltServer;
import indexingTopology.bolt.QueryCoordinatorBolt;
import indexingTopology.bolt.QueryCoordinatorWithQueryReceiverServerBolt;
import indexingTopology.config.TopologyConfig;
import indexingTopology.common.data.DataSchema;
import indexingTopology.common.data.DataTuple;
import indexingTopology.common.logics.DataTupleMapper;
import indexingTopology.topology.TopologyGenerator;
import indexingTopology.util.AvailableSocketPool;
import junit.framework.TestCase;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.generated.StormTopology;
import org.junit.Test;

import java.io.IOException;
import java.io.Serializable;
import java.util.Collections;
import java.util.function.Function;

/**
 * Created by robert on 10/3/17.
 */
public class TopologyOverallTest extends TestCase {

    TopologyConfig config = new TopologyConfig();
    AvailableSocketPool socketPool = new AvailableSocketPool();

    public void setUp() {
        try {
            Runtime.getRuntime().exec("mkdir -p ./target/tmp");
        } catch (IOException e) {
            e.printStackTrace();
        }
        config.HDFSFlag = false;
        config.dataChunkDir = "./target/tmp";
        config.metadataDir = "./target/tmp";
    }

    public void tearDown() {
        try {
            Runtime.getRuntime().exec("rm ./target/tmp/*");
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void testTopologyDouble() {
        boolean fullyTested = false;

        final String topologyName = "test_1";

        DataSchema schema = new DataSchema();
        schema.addDoubleField("f1");
        schema.addDoubleField("f2");
        schema.addDoubleField("f3");
        schema.addLongField("timestamp");
        schema.setPrimaryIndexField("f1");

        int ingestionPort = socketPool.getAvailablePort();
        int queryPort = socketPool.getAvailablePort();

        Double lowerBound = 0.0;
        Double upperBound = 5000.0;

        final boolean enableLoadBalance = false;


        InputStreamReceiverBolt dataSource = new InputStreamReceiverBoltServer(schema, ingestionPort, config);
        QueryCoordinatorBolt<Double> queryCoordinatorBolt = new QueryCoordinatorWithQueryReceiverServerBolt<>(lowerBound,
                upperBound, queryPort, config, schema);

        TopologyGenerator<Double> topologyGenerator = new TopologyGenerator<>();

        StormTopology topology = topologyGenerator.generateIndexingTopology(schema, lowerBound, upperBound,
                enableLoadBalance, dataSource, queryCoordinatorBolt, config);

        Config conf = new Config();
        conf.setDebug(false);
        conf.setNumWorkers(1);

        conf.put(Config.WORKER_CHILDOPTS, "-Xmx2048m");
//        conf.put(Config.SUPERVISOR_CHILDOPTS, "-Xmx2048m");
        conf.put(Config.WORKER_HEAP_MEMORY_MB, 2048);

        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology(topologyName, conf, topology);


        double start = 0;
        double step = 1.5;
        int numberOfTuples = 50;

        try {
//            Thread.sleep(10000);
            IngestionClient oneTuplePerTransferIngestionClient = new IngestionClient("localhost", ingestionPort);
            oneTuplePerTransferIngestionClient.connectWithTimeout(10000);
            for (int i = 0; i < numberOfTuples; i++) {
                oneTuplePerTransferIngestionClient.append(new DataTuple(start + step * i, 0.0, 0.0, System.currentTimeMillis()));
            }

            Thread.sleep(1000);

            QueryClient queryClient = new QueryClient("localhost", queryPort);
            queryClient.connectWithTimeout(10000);
            QueryResponse response = queryClient.temporalRangeQuery(0.0, 10000.0, 0, Long.MAX_VALUE);

            assertEquals(numberOfTuples, response.dataTuples.size());
            oneTuplePerTransferIngestionClient.close();
            queryClient.close();
            cluster.killTopology(topologyName);
            cluster.shutdown();
            Thread.sleep(1000);
            fullyTested = true;
//            Thread.sleep(5000);

        } catch (Exception e) {
            e.printStackTrace();
        }
        assertTrue(fullyTested);
        socketPool.returnPort(ingestionPort);
        socketPool.returnPort(queryPort);
    }

    @Test
    public void testTopologyIntegerFilter() {

        boolean fullyTested = false;
        final String topologyName = "test_2";

        DataSchema schema = new DataSchema();
        schema.addIntField("f1");
        schema.addIntField("f2");
        schema.addIntField("f3");
        schema.addLongField("timestamp");
        schema.setPrimaryIndexField("f1");

        int ingestionPort = socketPool.getAvailablePort();
        int queryPort = socketPool.getAvailablePort();

        Integer lowerBound = 0;
        Integer upperBound = 5000;

        final boolean enableLoadBalance = false;

        InputStreamReceiverBolt dataSource = new InputStreamReceiverBoltServer(schema, ingestionPort, config);
        QueryCoordinatorBolt<Integer> queryCoordinatorBolt = new QueryCoordinatorWithQueryReceiverServerBolt<>(lowerBound,
                upperBound, queryPort, config, schema);

        TopologyGenerator<Integer> topologyGenerator = new TopologyGenerator<>();

        StormTopology topology = topologyGenerator.generateIndexingTopology(schema, lowerBound, upperBound,
                enableLoadBalance, dataSource, queryCoordinatorBolt, config);

        Config conf = new Config();
        conf.setDebug(false);
        conf.setNumWorkers(1);

        conf.put(Config.WORKER_CHILDOPTS, "-Xmx2048m");
//        conf.put(Config.SUPERVISOR_CHILDOPTS, "-Xmx2048m");
        conf.put(Config.WORKER_HEAP_MEMORY_MB, 2048);

        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology(topologyName, conf, topology);


        int start = 0;
        int step = 1;
        int numberOfTuples = 50;

        try {
//            Thread.sleep(10000);
            IngestionClient oneTuplePerTransferIngestionClient = new IngestionClient("localhost", ingestionPort);
            oneTuplePerTransferIngestionClient.connectWithTimeout(10000);
            for (int i = 0; i < numberOfTuples; i++) {
                oneTuplePerTransferIngestionClient.append(new DataTuple(start + step * i, 0, 0, System.currentTimeMillis()));
            }

            Thread.sleep(1000);

            QueryClient queryClient = new QueryClient("localhost", queryPort);
            queryClient.connectWithTimeout(10000);
            QueryResponse response = queryClient.temporalRangeQuery(0, 40, 0, Long.MAX_VALUE);

            assertEquals(41, response.dataTuples.size());
            oneTuplePerTransferIngestionClient.close();
            queryClient.close();
            cluster.killTopology(topologyName);
            cluster.shutdown();
            Thread.sleep(1000);
            fullyTested = true;

        } catch (Exception e) {
            e.printStackTrace();
        }
        assertTrue(fullyTested);
        socketPool.returnPort(ingestionPort);
        socketPool.returnPort(queryPort);
    }


    @Test
    public void testTopologyDoubleFilter() {

        boolean fullyTested = false;

        final String topologyName = "test_3";

        DataSchema schema = new DataSchema();
        schema.addDoubleField("f1");
        schema.addDoubleField("f2");
        schema.addDoubleField("f3");
        schema.addLongField("timestamp");
        schema.setPrimaryIndexField("f1");

        int ingestionPort = socketPool.getAvailablePort();
        int queryPort = socketPool.getAvailablePort();

        Double lowerBound = 0.0;
        Double upperBound = 5000.0;

        final boolean enableLoadBalance = false;

        InputStreamReceiverBolt dataSource = new InputStreamReceiverBoltServer(schema, ingestionPort, config);
        QueryCoordinatorBolt<Double> queryCoordinatorBolt = new QueryCoordinatorWithQueryReceiverServerBolt<>(lowerBound,
                upperBound, queryPort, config, schema);

        TopologyGenerator<Double> topologyGenerator = new TopologyGenerator<>();

        StormTopology topology = topologyGenerator.generateIndexingTopology(schema, lowerBound, upperBound,
                enableLoadBalance, dataSource, queryCoordinatorBolt, config);

        Config conf = new Config();
        conf.setDebug(false);
        conf.setNumWorkers(1);

        conf.put(Config.WORKER_CHILDOPTS, "-Xmx2048m");
//        conf.put(Config.SUPERVISOR_CHILDOPTS, "-Xmx2048m");
        conf.put(Config.WORKER_HEAP_MEMORY_MB, 2048);

        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology(topologyName, conf, topology);


        double start = 0;
        double step = 2.0;
        int numberOfTuples = 50;

        try {
//            Thread.sleep(10000);
            IngestionClient oneTuplePerTransferIngestionClient = new IngestionClient("localhost", ingestionPort);
            oneTuplePerTransferIngestionClient.connectWithTimeout(10000);
            for (int i = 0; i < numberOfTuples; i++) {
                oneTuplePerTransferIngestionClient.append(new DataTuple(start + step * i, 0.0, 0.0, System.currentTimeMillis()));
            }

            Thread.sleep(1000);

            QueryClient queryClient = new QueryClient("localhost", queryPort);
            queryClient.connectWithTimeout(10000);
            QueryResponse response = queryClient.temporalRangeQuery(0.0, 9.5, 0, Long.MAX_VALUE);

            assertEquals(5, response.dataTuples.size());
            oneTuplePerTransferIngestionClient.close();
            queryClient.close();
            cluster.killTopology(topologyName);
            cluster.shutdown();
            Thread.sleep(1000);
            fullyTested = true;
        } catch (Exception e) {
            e.printStackTrace();
        }
        assertTrue(fullyTested);
        socketPool.returnPort(ingestionPort);
        socketPool.returnPort(queryPort);
    }

    @Test
    public void testTopologyDoubleFilterWithPredicate() {

        boolean fullyTested = false;

        final String topologyName = "test_4";

        DataSchema schema = new DataSchema();
        schema.addDoubleField("f1");
        schema.addDoubleField("f2");
        schema.addDoubleField("f3");
        schema.addLongField("timestamp");
        schema.setPrimaryIndexField("f1");

        int ingestionPort = socketPool.getAvailablePort();
        int queryPort = socketPool.getAvailablePort();

        Double lowerBound = 0.0;
        Double upperBound = 5000.0;

        final boolean enableLoadBalance = false;

        InputStreamReceiverBolt dataSource = new InputStreamReceiverBoltServer(schema, ingestionPort, config);
        QueryCoordinatorBolt<Double> queryCoordinatorBolt = new QueryCoordinatorWithQueryReceiverServerBolt<>(lowerBound,
                upperBound, queryPort, config, schema);

        TopologyGenerator<Double> topologyGenerator = new TopologyGenerator<>();

        StormTopology topology = topologyGenerator.generateIndexingTopology(schema, lowerBound, upperBound,
                enableLoadBalance, dataSource, queryCoordinatorBolt, config);

        Config conf = new Config();
        conf.setDebug(false);
        conf.setNumWorkers(1);

        conf.put(Config.WORKER_CHILDOPTS, "-Xmx2048m");
//        conf.put(Config.SUPERVISOR_CHILDOPTS, "-Xmx2048m");
        conf.put(Config.WORKER_HEAP_MEMORY_MB, 2048);

        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology(topologyName, conf, topology);


        double start = 0;
        double step = 2.0;
        int numberOfTuples = 50;

        try {
//            Thread.sleep(10000);
            IngestionClient oneTuplePerTransferIngestionClient = new IngestionClient("localhost", ingestionPort);
            oneTuplePerTransferIngestionClient.connectWithTimeout(10000);
            for (int i = 0; i < numberOfTuples; i++) {
                oneTuplePerTransferIngestionClient.append(new DataTuple(start + step * i, 0.0, 0.0, System.currentTimeMillis()));
            }

            Thread.sleep(1000);

            QueryClient queryClient = new QueryClient("localhost", queryPort);
            queryClient.connectWithTimeout(10000);
//            QueryResponse response = queryClient.temporalRangeQuery(0.0, 9.5, 0, Long.MAX_VALUE);
            QueryResponse response = queryClient.query(new QueryRequest<>(0.0, 9.5, 0, Long.MAX_VALUE, t -> (Double)schema.getValue("f1", t) > 5, null));

            assertEquals(2, response.dataTuples.size());

            oneTuplePerTransferIngestionClient.close();
            queryClient.close();
            cluster.killTopology(topologyName);
            cluster.shutdown();
            Thread.sleep(1000);
            fullyTested = true;
        } catch (Exception e) {
            e.printStackTrace();
        }
        assertTrue(fullyTested);
        socketPool.returnPort(ingestionPort);
        socketPool.returnPort(queryPort);
    }

    @Test
    public void testTopologyAggregation() {

        boolean fullyTested = false;

        final String topologyName = "test_4";

        DataSchema schema = new DataSchema();
        schema.addIntField("f1");
        schema.addIntField("f2");
        schema.addIntField("f3");
        schema.addLongField("timestamp");
        schema.setPrimaryIndexField("f1");

        int ingestionPort = socketPool.getAvailablePort();
        int queryPort = socketPool.getAvailablePort();

        Integer lowerBound = 0;
        Integer upperBound = 5000;

        final boolean enableLoadBalance = false;

        InputStreamReceiverBolt dataSource = new InputStreamReceiverBoltServer(schema, ingestionPort, config);
        QueryCoordinatorBolt<Integer> queryCoordinatorBolt = new QueryCoordinatorWithQueryReceiverServerBolt<>(lowerBound,
                upperBound, queryPort, config, schema);

        TopologyGenerator<Integer> topologyGenerator = new TopologyGenerator<>();

        StormTopology topology = topologyGenerator.generateIndexingTopology(schema, lowerBound, upperBound,
                enableLoadBalance, dataSource, queryCoordinatorBolt, config);

        Config conf = new Config();
        conf.setDebug(false);
        conf.setNumWorkers(1);

        conf.put(Config.WORKER_CHILDOPTS, "-Xmx2048m");
//        conf.put(Config.SUPERVISOR_CHILDOPTS, "-Xmx2048m");
        conf.put(Config.WORKER_HEAP_MEMORY_MB, 2048);

        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology(topologyName, conf, topology);


        int start = 0;
        int step = 2;
        int numberOfTuples = 50;

        try {
//            Thread.sleep(10000);
            IngestionClient oneTuplePerTransferIngestionClient = new IngestionClient("localhost", ingestionPort);
            oneTuplePerTransferIngestionClient.connectWithTimeout(10000);
            for (int i = 0; i < numberOfTuples; i++) {
                oneTuplePerTransferIngestionClient.append(new DataTuple(start + step * i, i % 10, 4, System.currentTimeMillis()));
            }

            Thread.sleep(1000);

            QueryClient queryClient = new QueryClient("localhost", queryPort);
            queryClient.connectWithTimeout(10000);

            Aggregator<Integer> aggregator = new Aggregator<>(schema, "f2", new AggregateField[]{
                    new AggregateField(new Sum(), "f3"), new AggregateField(new Count(), "f3")
            });

            QueryResponse response = queryClient.query(new QueryRequest<>(0, 100, 0L, Long.MAX_VALUE, null, aggregator));
            Collections.sort(response.dataTuples, (DataTuple t1, DataTuple t2) -> ((Comparable)t1.get(0)).compareTo(t2.get(0)));
            assertEquals(10, response.dataTuples.size());
            assertEquals(new DataTuple(0, 20.0, 5.0), response.dataTuples.get(0));
            assertEquals(new DataTuple(1, 20.0, 5.0), response.dataTuples.get(1));
            assertEquals(new DataTuple(2, 20.0, 5.0), response.dataTuples.get(2));
            assertEquals(new DataTuple(3, 20.0, 5.0), response.dataTuples.get(3));
            assertEquals(new DataTuple(4, 20.0, 5.0), response.dataTuples.get(4));
            assertEquals(new DataTuple(5, 20.0, 5.0), response.dataTuples.get(5));
            assertEquals(new DataTuple(6, 20.0, 5.0), response.dataTuples.get(6));
            assertEquals(new DataTuple(7, 20.0, 5.0), response.dataTuples.get(7));
            assertEquals(new DataTuple(8, 20.0, 5.0), response.dataTuples.get(8));
            assertEquals(new DataTuple(9, 20.0, 5.0), response.dataTuples.get(9));

            oneTuplePerTransferIngestionClient.close();
            queryClient.close();
            cluster.killTopology(topologyName);
            cluster.shutdown();
            Thread.sleep(1000);
            fullyTested = true;
        } catch (Exception e) {
            e.printStackTrace();
        }
        assertTrue(fullyTested);
        socketPool.returnPort(ingestionPort);
        socketPool.returnPort(queryPort);
    }

    @Test
    public void testTopologyIntegerFilterWithTrivialMapper() {

        boolean fullyTested = false;
        final String topologyName = "test_5";

        DataSchema schema = new DataSchema();
        schema.addIntField("f1");
        schema.addIntField("f2");
        schema.addIntField("f3");
        schema.addLongField("timestamp");
        schema.setPrimaryIndexField("f1");

        int ingestionPort = socketPool.getAvailablePort();
        int queryPort = socketPool.getAvailablePort();

        Integer lowerBound = 0;
        Integer upperBound = 5000;

        final boolean enableLoadBalance = false;

        InputStreamReceiverBolt dataSource = new InputStreamReceiverBoltServer(schema, ingestionPort, config);
        QueryCoordinatorBolt<Integer> queryCoordinatorBolt = new QueryCoordinatorWithQueryReceiverServerBolt<>(lowerBound,
                upperBound, queryPort, config, schema);

        TopologyGenerator<Integer> topologyGenerator = new TopologyGenerator<>();

        DataTupleMapper dataTupleMapper = new DataTupleMapper(schema, (Serializable & Function<DataTuple, DataTuple>) t -> t);

        StormTopology topology = topologyGenerator.generateIndexingTopology(schema, lowerBound, upperBound,
                enableLoadBalance, dataSource, queryCoordinatorBolt, dataTupleMapper, config);

        Config conf = new Config();
        conf.setDebug(false);
        conf.setNumWorkers(1);

        conf.put(Config.WORKER_CHILDOPTS, "-Xmx2048m");
//        conf.put(Config.SUPERVISOR_CHILDOPTS, "-Xmx2048m");
        conf.put(Config.WORKER_HEAP_MEMORY_MB, 2048);

        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology(topologyName, conf, topology);


        int start = 0;
        int step = 1;
        int numberOfTuples = 50;

        try {
//            Thread.sleep(10000);
            IngestionClient oneTuplePerTransferIngestionClient = new IngestionClient("localhost", ingestionPort);
            oneTuplePerTransferIngestionClient.connectWithTimeout(10000);
            for (int i = 0; i < numberOfTuples; i++) {
                oneTuplePerTransferIngestionClient.append(new DataTuple(start + step * i, 0, 0, System.currentTimeMillis()));
            }

            Thread.sleep(1000);

            QueryClient queryClient = new QueryClient("localhost", queryPort);
            queryClient.connectWithTimeout(10000);
            QueryResponse response = queryClient.temporalRangeQuery(0, 40, 0, Long.MAX_VALUE);

            assertEquals(41, response.dataTuples.size());
            oneTuplePerTransferIngestionClient.close();
            queryClient.close();
            cluster.killTopology(topologyName);
            cluster.shutdown();
            Thread.sleep(1000);
            fullyTested = true;

        } catch (Exception e) {
            e.printStackTrace();
        }
        assertTrue(fullyTested);
        socketPool.returnPort(ingestionPort);
        socketPool.returnPort(queryPort);
    }
}
