package indexingTopology;

import indexingTopology.aggregator.AggregateField;
import indexingTopology.aggregator.Aggregator;
import indexingTopology.aggregator.Count;
import indexingTopology.aggregator.Sum;
import indexingTopology.bolt.InputStreamReceiver;
import indexingTopology.bolt.InputStreamReceiverServer;
import indexingTopology.bolt.QueryCoordinator;
import indexingTopology.bolt.QueryCoordinatorWithQueryReceiverServer;
import indexingTopology.client.*;
import indexingTopology.data.DataSchema;
import indexingTopology.data.DataTuple;
import indexingTopology.util.DataTupleMapper;
import indexingTopology.util.TopologyGenerator;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.generated.KillOptions;
import org.apache.storm.generated.StormTopology;
import org.junit.Test;
import static org.junit.Assert.*;

import java.io.Serializable;
import java.util.Collections;
import java.util.function.Consumer;
import java.util.function.Function;

/**
 * Created by robert on 10/3/17.
 */
public class TopologyOverallTest {

    @Test
    public void testTopologyDouble() {
        final int ingestionPort = 10000;
        final int queryPort = 10001;
        final String topologyName = "test_1";

        DataSchema schema = new DataSchema();
        schema.addDoubleField("f1");
        schema.addDoubleField("f2");
        schema.addDoubleField("f3");
        schema.addLongField("timestamp");
        schema.setPrimaryIndexField("f1");


        Double lowerBound = 0.0;
        Double upperBound = 5000.0;

        final boolean enableLoadBalance = false;


        InputStreamReceiver dataSource = new InputStreamReceiverServer(schema, ingestionPort);
        QueryCoordinator<Double> queryCoordinator = new QueryCoordinatorWithQueryReceiverServer<>(lowerBound, upperBound, queryPort);

        TopologyGenerator<Double> topologyGenerator = new TopologyGenerator<>();

        StormTopology topology = topologyGenerator.generateIndexingTopology(schema, lowerBound, upperBound, enableLoadBalance, dataSource, queryCoordinator);

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

            QueryClientSkeleton queryClient = new QueryClientSkeleton("localhost", queryPort);
            queryClient.connectWithTimeout(10000);
            QueryResponse response = queryClient.temporalRangeQuery(0.0, 10000.0, 0, Long.MAX_VALUE);

            assertEquals(numberOfTuples, response.dataTuples.size());
            KillOptions killOptions = new KillOptions();
            killOptions.set_wait_secs(0);
            cluster.killTopology(topologyName);
//            Thread.sleep(5000);

        } catch (Exception e) {
            e.printStackTrace();
        }

    }

    @Test
    public void testTopologyIntegerFilter() {

        final int ingestionPort = 10010;
        final int queryPort = 10011;
        final String topologyName = "test_2";

        DataSchema schema = new DataSchema();
        schema.addIntField("f1");
        schema.addIntField("f2");
        schema.addIntField("f3");
        schema.addLongField("timestamp");
        schema.setPrimaryIndexField("f1");


        Integer lowerBound = 0;
        Integer upperBound = 5000;

        final boolean enableLoadBalance = false;

        InputStreamReceiver dataSource = new InputStreamReceiverServer(schema, ingestionPort);
        QueryCoordinator<Integer> queryCoordinator = new QueryCoordinatorWithQueryReceiverServer<>(lowerBound, upperBound, queryPort);

        TopologyGenerator<Integer> topologyGenerator = new TopologyGenerator<>();

        StormTopology topology = topologyGenerator.generateIndexingTopology(schema, lowerBound, upperBound, enableLoadBalance, dataSource, queryCoordinator);

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

            QueryClientSkeleton queryClient = new QueryClientSkeleton("localhost", queryPort);
            queryClient.connectWithTimeout(10000);
            QueryResponse response = queryClient.temporalRangeQuery(0, 40, 0, Long.MAX_VALUE);

            assertEquals(41, response.dataTuples.size());
            KillOptions killOptions = new KillOptions();
            killOptions.set_wait_secs(0);
            cluster.killTopology(topologyName);
            Thread.sleep(5000);


        } catch (Exception e) {
            e.printStackTrace();
        }

    }


    @Test
    public void testTopologyDoubleFilter() {

        final int ingestionPort = 10020;
        final int queryPort = 10021;
        final String topologyName = "test_3";

        DataSchema schema = new DataSchema();
        schema.addDoubleField("f1");
        schema.addDoubleField("f2");
        schema.addDoubleField("f3");
        schema.addLongField("timestamp");
        schema.setPrimaryIndexField("f1");


        Double lowerBound = 0.0;
        Double upperBound = 5000.0;

        final boolean enableLoadBalance = false;

        InputStreamReceiver dataSource = new InputStreamReceiverServer(schema, ingestionPort);
        QueryCoordinator<Double> queryCoordinator = new QueryCoordinatorWithQueryReceiverServer<>(lowerBound, upperBound, queryPort);

        TopologyGenerator<Double> topologyGenerator = new TopologyGenerator<>();

        StormTopology topology = topologyGenerator.generateIndexingTopology(schema, lowerBound, upperBound, enableLoadBalance, dataSource, queryCoordinator);

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

            QueryClientSkeleton queryClient = new QueryClientSkeleton("localhost", queryPort);
            queryClient.connectWithTimeout(10000);
            QueryResponse response = queryClient.temporalRangeQuery(0.0, 9.5, 0, Long.MAX_VALUE);

            assertEquals(5, response.dataTuples.size());
            KillOptions killOptions = new KillOptions();
            killOptions.set_wait_secs(0);
            cluster.killTopology(topologyName);
            Thread.sleep(5000);

        } catch (Exception e) {
            e.printStackTrace();
        }

    }

    @Test
    public void testTopologyDoubleFilterWithPredicate() {

        final int ingestionPort = 10030;
        final int queryPort = 10031;
        final String topologyName = "test_4";

        DataSchema schema = new DataSchema();
        schema.addDoubleField("f1");
        schema.addDoubleField("f2");
        schema.addDoubleField("f3");
        schema.addLongField("timestamp");
        schema.setPrimaryIndexField("f1");


        Double lowerBound = 0.0;
        Double upperBound = 5000.0;

        final boolean enableLoadBalance = false;

        InputStreamReceiver dataSource = new InputStreamReceiverServer(schema, ingestionPort);
        QueryCoordinator<Double> queryCoordinator = new QueryCoordinatorWithQueryReceiverServer<>(lowerBound, upperBound, queryPort);

        TopologyGenerator<Double> topologyGenerator = new TopologyGenerator<>();

        StormTopology topology = topologyGenerator.generateIndexingTopology(schema, lowerBound, upperBound, enableLoadBalance, dataSource, queryCoordinator);

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

            QueryClientSkeleton queryClient = new QueryClientSkeleton("localhost", queryPort);
            queryClient.connectWithTimeout(10000);
//            QueryResponse response = queryClient.temporalRangeQuery(0.0, 9.5, 0, Long.MAX_VALUE);
            QueryResponse response = queryClient.query(new QueryRequest<>(0.0, 9.5, 0, Long.MAX_VALUE, t -> (Double)schema.getValue("f1", t) > 5, null));

            assertEquals(2, response.dataTuples.size());


            KillOptions killOptions = new KillOptions();
            killOptions.set_wait_secs(0);
            cluster.killTopology(topologyName);
            Thread.sleep(5000);

        } catch (Exception e) {
            e.printStackTrace();
        }

    }

    @Test
    public void testTopologyAggregation() {

        final int ingestionPort = 10040;
        final int queryPort = 10041;
        final String topologyName = "test_4";

        DataSchema schema = new DataSchema();
        schema.addIntField("f1");
        schema.addIntField("f2");
        schema.addIntField("f3");
        schema.addLongField("timestamp");
        schema.setPrimaryIndexField("f1");


        Integer lowerBound = 0;
        Integer upperBound = 5000;

        final boolean enableLoadBalance = false;

        InputStreamReceiver dataSource = new InputStreamReceiverServer(schema, ingestionPort);
        QueryCoordinator<Integer> queryCoordinator = new QueryCoordinatorWithQueryReceiverServer<>(lowerBound, upperBound, queryPort);

        TopologyGenerator<Integer> topologyGenerator = new TopologyGenerator<>();

        StormTopology topology = topologyGenerator.generateIndexingTopology(schema, lowerBound, upperBound, enableLoadBalance, dataSource, queryCoordinator);

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

            QueryClientSkeleton queryClient = new QueryClientSkeleton("localhost", queryPort);
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


            KillOptions killOptions = new KillOptions();
            killOptions.set_wait_secs(0);
            cluster.killTopology(topologyName);
            Thread.sleep(5000);

        } catch (Exception e) {
            e.printStackTrace();
        }

    }

    @Test
    public void testTopologyIntegerFilterWithTrivialMapper() {

        final int ingestionPort = 10050;
        final int queryPort = 10051;
        final String topologyName = "test_5";

        DataSchema schema = new DataSchema();
        schema.addIntField("f1");
        schema.addIntField("f2");
        schema.addIntField("f3");
        schema.addLongField("timestamp");
        schema.setPrimaryIndexField("f1");


        Integer lowerBound = 0;
        Integer upperBound = 5000;

        final boolean enableLoadBalance = false;

        InputStreamReceiver dataSource = new InputStreamReceiverServer(schema, ingestionPort);
        QueryCoordinator<Integer> queryCoordinator = new QueryCoordinatorWithQueryReceiverServer<>(lowerBound, upperBound, queryPort);

        TopologyGenerator<Integer> topologyGenerator = new TopologyGenerator<>();

        DataTupleMapper dataTupleMapper = new DataTupleMapper(schema, (Serializable & Function<DataTuple, DataTuple>) t -> t);

        StormTopology topology = topologyGenerator.generateIndexingTopology(schema, lowerBound, upperBound,
                enableLoadBalance, dataSource, queryCoordinator, dataTupleMapper);

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

            QueryClientSkeleton queryClient = new QueryClientSkeleton("localhost", queryPort);
            queryClient.connectWithTimeout(10000);
            QueryResponse response = queryClient.temporalRangeQuery(0, 40, 0, Long.MAX_VALUE);

            assertEquals(41, response.dataTuples.size());
            KillOptions killOptions = new KillOptions();
            killOptions.set_wait_secs(0);
            cluster.killTopology(topologyName);
            Thread.sleep(5000);


        } catch (Exception e) {
            e.printStackTrace();
        }

    }
}
