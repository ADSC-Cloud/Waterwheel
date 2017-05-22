package indexingTopology.topology.kingbase;

import indexingTopology.aggregator.AggregateField;
import indexingTopology.aggregator.Aggregator;
import indexingTopology.aggregator.Count;
import indexingTopology.bolt.GeoTemporalQueryCoordinatorWithQueryReceiverServer;
import indexingTopology.bolt.InputStreamReceiver;
import indexingTopology.bolt.InputStreamReceiverServer;
import indexingTopology.bolt.QueryCoordinator;
import indexingTopology.client.GeoTemporalQueryClient;
import indexingTopology.client.GeoTemporalQueryRequest;
import indexingTopology.client.IngestionClientBatchMode;
import indexingTopology.client.QueryResponse;
import indexingTopology.config.TopologyConfig;
import indexingTopology.data.DataSchema;
import indexingTopology.data.DataTuple;
import indexingTopology.util.*;
import indexingTopology.util.taxi.*;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.AlreadyAliveException;
import org.apache.storm.generated.AuthorizationException;
import org.apache.storm.generated.InvalidTopologyException;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.metric.internal.RateTracker;
import org.apache.storm.utils.Utils;

import java.io.IOException;
import java.io.Serializable;
import java.net.SocketTimeoutException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.function.Function;
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;



/**
 * Created by acelzj on 11/15/16.
 */
public class KingBaseTopology {

    /**
     * general configuration
     */
    @Option(name = "--help", aliases = {"-h"}, usage = "help")
    private boolean Help = false;

    @Option(name = "--mode", aliases = {"-m"}, usage = "submit|ingest|query")
    private String Mode = "Not Given";

    /**
     * topology configuration
     */
    @Option(name = "--topology-name", aliases = "-t", usage = "topology name")
    private String TopologyName = "T0";

    @Option(name = "--node", aliases = {"-n"}, usage = "number of nodes used in the topology")
    private int NumberOfNodes = 1;

    @Option(name = "--local", usage = "run the topology in local cluster")
    private boolean LocalMode = false;

    /**
     * ingest client configuration
     */
    @Option(name = "--ingest-server-ip", usage = "the ingestion server ip")
    private String IngestServerIp = "localhost";

    @Option(name = "--ingest-rate-limit", aliases = {"-r"}, usage = "max ingestion rate")
    private int MaxIngestRate = Integer.MAX_VALUE;

    /**
     * query client configuration
     */
    @Option(name = "--query-server-ip", usage = "the query server ip")
    private String QueryServerIp = "localhost";

    @Option(name = "--selectivity", usage = "the selectivity on the key domain")
    private double Selectivity = 0.01;

    @Option(name = "--temporal", usage = "recent time in seconds of interest")
    private int RecentSecondsOfInterest = 5;

    @Option(name = "--queries", usage = "number of queries to perform")
    private int NumberOfQueries = Integer.MAX_VALUE;


    static final double x1 = 40.012928;
    static final double x2 = 40.023983;
    static final double y1 = 116.292677;
    static final double y2 = 116.614865;
    static final int partitions = 128;

    public void executeQuery() {

        double selectivityOnOneDimension = Math.sqrt(Selectivity);
        DataSchema schema = getDataSchema();
        GeoTemporalQueryClient queryClient = new GeoTemporalQueryClient(QueryServerIp, 10001);
        Thread queryThread = new Thread(() -> {
            try {
                queryClient.connectWithTimeout(10000);
            } catch (IOException e) {
                e.printStackTrace();
            }
            Random random = new Random();

            int executed = 0;
            long totalQueryTime = 0;

            while (true) {

                double x = x1 + (x2 - x1) * (1 - selectivityOnOneDimension) * random.nextDouble();
                double y = y1 + (y2 - y1) * (1 - selectivityOnOneDimension) * random.nextDouble();

                final Double xLow = x;
                final Double xHigh = x + selectivityOnOneDimension * (x2 - x1);
                final Double yLow = y;
                final Double yHigh = y + selectivityOnOneDimension * (y2 - y1);

//                DataTuplePredicate predicate = t ->
//                                 (double) schema.getValue("lon", t) >= xLow &&
//                                (double) schema.getValue("lon", t) <= xHigh &&
//                                (double) schema.getValue("lat", t) >= yLow &&
//                                (double) schema.getValue("lat", t) <= yHigh ;

//                DataTuplePredicate predicate = t -> schema.getValue("id", t).equals(Integer.toString(new Random().nextInt(100000)));
                DataTuplePredicate predicate = t -> schema.getValue("id", t).equals(Integer.toString(1000));


                Aggregator<Integer> aggregator = new Aggregator<>(schema, null, new AggregateField(new Count(), "*"));
//                Aggregator<Integer> aggregator = null;


//                DataSchema schemaAfterAggregation = aggregator.getOutputDataSchema();
//                DataTupleSorter sorter = (DataTuple o1, DataTuple o2) -> Double.compare((double) schemaAfterAggregation.getValue("count(*)", o1),
//                        (double) schemaAfterAggregation.getValue("count(*)", o2));


                DataTupleEquivalentPredicateHint equivalentPredicateHint = new DataTupleEquivalentPredicateHint("id", "1000");

                GeoTemporalQueryRequest queryRequest = new GeoTemporalQueryRequest<>(xLow, xHigh, yLow, yHigh,
                        System.currentTimeMillis() - RecentSecondsOfInterest * 1000, System.currentTimeMillis(), predicate, aggregator, null, equivalentPredicateHint);
                long start = System.currentTimeMillis();
                try {
                    System.out.println("A query will be issued.");
                    QueryResponse response = queryClient.query(queryRequest);
                    System.out.println("A query finished.");
                    long end = System.currentTimeMillis();
                    totalQueryTime += end - start;
                    System.out.println(response);
                    System.out.println(String.format("Query time: %d ms", end - start));

                    if (executed++ >= NumberOfQueries) {
                        System.out.println("Average Query Latency: " + totalQueryTime / (double)executed);
                        break;
                    }

                } catch (SocketTimeoutException e) {
                    Thread.interrupted();
                } catch (IOException e) {
                    if (Thread.currentThread().interrupted()) {
                        Thread.interrupted();
                    }
                    e.printStackTrace();
                } catch (ClassNotFoundException e) {
                    e.printStackTrace();
                }

            }
            try {
                queryClient.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        });
        queryThread.start();
    }

    public void executeIngestion() {

        DataSchema rawSchema = getRawDataSchema();
        TrajectoryGenerator generator = new TrajectoryMovingGenerator(x1, x2, y1, y2, 100000, 45.0);
        IngestionClientBatchMode clientBatchMode = new IngestionClientBatchMode(IngestServerIp, 10000,
                rawSchema, 10240);

        RateTracker rateTracker = new RateTracker(1000,2);
        FrequencyRestrictor restrictor = new FrequencyRestrictor(MaxIngestRate, 5);

        Thread ingestionThread = new Thread(()->{
            Random random = new Random();

            try {
                clientBatchMode.connectWithTimeout(10000);
            } catch (IOException e) {
                e.printStackTrace();
            }
            while(true) {
                Car car = generator.generate();
                DataTuple tuple = new DataTuple();
                tuple.add(Integer.toString((int)car.id));
                tuple.add(Integer.toString(random.nextInt()));
                tuple.add(car.x);
                tuple.add(car.y);
                tuple.add(1);
                tuple.add(55.3);
                tuple.add("position 1");
                tuple.add("2015-10-10, 11:12:34");
                try {
                    restrictor.getPermission();
                    clientBatchMode.appendInBatch(tuple);
                    rateTracker.notify(1);
                    if(Thread.interrupted()) {
                        break;
                    }
                } catch (IOException e) {
                    if (clientBatchMode.isClosed())
                        break;
                    e.printStackTrace();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }

//                try {
////                    Thread.sleep(1);
//                } catch (InterruptedException e) {
//                    e.printStackTrace();
//                }

            }
        });
        ingestionThread.start();

        new Thread(()->{
            while (true) {
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                    break;
                }
                System.out.println("Insertion throughput: " + rateTracker.reportRate());
            }
        }).start();
    }

    public void submitTopology() throws InvalidTopologyException, AuthorizationException, AlreadyAliveException {
        DataSchema rawSchema = getRawDataSchema();
        DataSchema schema = getDataSchema();
        City city = new City(x1, x2, y1, y2, partitions);

        Integer lowerBound = 0;
        Integer upperBound = city.getMaxZCode();

        final boolean enableLoadBalance = false;

        TopologyConfig config = new TopologyConfig();

        InputStreamReceiver dataSource = new InputStreamReceiverServer(rawSchema, 10000, config);

        QueryCoordinator<Integer> queryCoordinator = new GeoTemporalQueryCoordinatorWithQueryReceiverServer<>(lowerBound,
                upperBound, 10001, city, config);

        DataTupleMapper dataTupleMapper = new DataTupleMapper(rawSchema, (Serializable & Function<DataTuple, DataTuple>) t -> {
            double lon = (double)schema.getValue("lon", t);
            double lat = (double)schema.getValue("lat", t);
            int zcode = city.getZCodeForALocation(lon, lat);
            t.add(zcode);
            t.add(System.currentTimeMillis());
            return t;
        });

        List<String> bloomFilterColumns = new ArrayList<>();
        bloomFilterColumns.add("id");

        TopologyGenerator<Integer> topologyGenerator = new TopologyGenerator<>();
        topologyGenerator.setNumberOfNodes(NumberOfNodes);

        StormTopology topology = topologyGenerator.generateIndexingTopology(schema, lowerBound, upperBound,
                enableLoadBalance, dataSource, queryCoordinator, dataTupleMapper, bloomFilterColumns, config);

        Config conf = new Config();
        conf.setDebug(false);
        conf.setNumWorkers(NumberOfNodes);

        conf.put(Config.WORKER_CHILDOPTS, "-Xmx2048m");
        conf.put(Config.WORKER_HEAP_MEMORY_MB, 2048);

        if (LocalMode) {
            LocalCluster localCluster = new LocalCluster();
            localCluster.submitTopology(TopologyName, conf, topology);
        } else {
            StormSubmitter.submitTopology(TopologyName, conf, topology);
        }
    }

    public static void main(String[] args) throws InvalidTopologyException, AuthorizationException, AlreadyAliveException {

        KingBaseTopology kingBaseTopology = new KingBaseTopology();

        CmdLineParser parser = new CmdLineParser(kingBaseTopology);

        try {
            parser.parseArgument(args);
        } catch (CmdLineException e) {
            e.printStackTrace();
            parser.printUsage(System.out);
        }

        if (kingBaseTopology.Help) {
            parser.printUsage(System.out);
        }

        switch (kingBaseTopology.Mode) {
            case "submit": kingBaseTopology.submitTopology(); break;
            case "ingest": kingBaseTopology.executeIngestion(); break;
            case "query": kingBaseTopology.executeQuery(); break;
            default: System.out.println("Invalid command!");
        }
//        if (command.equals("submit"))
//            submitTopology();
//        else if (command.equals("ingest"))
//            executeIngestion();
//        else if (command.equals("query"))
//            executeQuery();

    }

    public static void Fakemain(String[] args) throws Exception {

        DataSchema rawSchema = getRawDataSchema();
        DataSchema schema = getDataSchema();


        double selectivity = Math.sqrt(1);

//        TrajectoryGenerator generator = new TrajectoryUniformGenerator(10000, x1, x2, y1, y2);
        TrajectoryGenerator generator = new TrajectoryMovingGenerator(x1, x2, y1, y2, 100000, 45.0);
        City city = new City(x1, x2, y1, y2, partitions);

        Integer lowerBound = 0;
        Integer upperBound = city.getMaxZCode();

        final boolean enableLoadBalance = false;

        TopologyConfig config = new TopologyConfig();

        InputStreamReceiver dataSource = new InputStreamReceiverServer(rawSchema, 10000, config);

        QueryCoordinator<Integer> queryCoordinator = new GeoTemporalQueryCoordinatorWithQueryReceiverServer<>(lowerBound,
                upperBound, 10001, city, config);


        DataTupleMapper dataTupleMapper = new DataTupleMapper(rawSchema, (Serializable & Function<DataTuple, DataTuple>) t -> {
            double lon = (double)schema.getValue("lon", t);
            double lat = (double)schema.getValue("lat", t);
            int zcode = city.getZCodeForALocation(lon, lat);
            t.add(zcode);
            t.add(System.currentTimeMillis());
            return t;
        });

        List<String> bloomFilterColumns = new ArrayList<>();
        bloomFilterColumns.add("id");

        TopologyGenerator<Integer> topologyGenerator = new TopologyGenerator<>();
        StormTopology topology = topologyGenerator.generateIndexingTopology(schema, lowerBound, upperBound,
                enableLoadBalance, dataSource, queryCoordinator, dataTupleMapper, bloomFilterColumns, config);

        Config conf = new Config();
        conf.setDebug(false);
        conf.setNumWorkers(1);

        conf.put(Config.WORKER_CHILDOPTS, "-Xmx2048m");
        conf.put(Config.WORKER_HEAP_MEMORY_MB, 2048);


        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology("T0", conf, topology);

        IngestionClientBatchMode clientBatchMode = new IngestionClientBatchMode("localhost", 10000,
                rawSchema, 1024);;
        Thread ingestionThread = new Thread(()->{
            Random random = new Random();

            try {
                clientBatchMode.connectWithTimeout(10000);
            } catch (IOException e) {
                e.printStackTrace();
            }
            while(true) {
                Car car = generator.generate();
                DataTuple tuple = new DataTuple();
                tuple.add(Integer.toString((int)car.id));
                tuple.add(Integer.toString(random.nextInt()));
                tuple.add(car.x);
                tuple.add(car.y);
                tuple.add(1);
                tuple.add(55.3);
                tuple.add("position 1");
                tuple.add("2015-10-10, 11:12:34");
                try {
                    clientBatchMode.appendInBatch(tuple);
                    if(Thread.interrupted()) {
                        break;
                    }
                } catch (IOException e) {
                    if (clientBatchMode.isClosed())
                        break;
                    e.printStackTrace();
                }

//                try {
////                    Thread.sleep(1);
//                } catch (InterruptedException e) {
//                    e.printStackTrace();
//                }

            }
        });
        ingestionThread.start();

        GeoTemporalQueryClient queryClient = new GeoTemporalQueryClient("localhost", 10001);
        Thread queryThread = new Thread(() -> {
            try {
                queryClient.connectWithTimeout(10000);
            } catch (IOException e) {
                e.printStackTrace();
            }
            Random random = new Random();

            while (true) {

                double x = x1 + (x2 - x1) * random.nextDouble();
                double y = y1 + (y2 - y1) * random.nextDouble();

                final Double xLow = x;
                final Double xHigh = x + selectivity * (x2 - x1);
                final Double yLow = y;
                final Double yHigh = y + selectivity * (y2 - y1);

                DataTuplePredicate predicate = new DataTuplePredicate() {
                    @Override
                    public boolean test(DataTuple objects) {
                        return (double)schema.getValue("lon", objects) >= xLow &&
                                (double)schema.getValue("lon", objects) <= xHigh &&
                                (double)schema.getValue("lat", objects) >= yLow &&
                                (double)schema.getValue("lat", objects) <= yHigh &&
                                schema.getValue("id", objects).equals("100");
                    }
                };

                Aggregator<Integer> aggregator = new Aggregator<>(schema, "zcode", new AggregateField(new Count(), "*"));
                DataSchema schemaAfterAggregation = aggregator.getOutputDataSchema();

                DataTupleSorter sorter = new DataTupleSorter() {
                    @Override
                    public int compare(DataTuple o1, DataTuple o2) {
                        return Double.compare((double)schemaAfterAggregation.getValue("count(*)", o1),
                                (double)schemaAfterAggregation.getValue("count(*)", o2));
                    }
                };

                DataTupleEquivalentPredicateHint equivalentPredicate = new DataTupleEquivalentPredicateHint("id", "100");

                GeoTemporalQueryRequest queryRequest = new GeoTemporalQueryRequest<>(xLow, xHigh, yLow, yHigh,
                        System.currentTimeMillis() - 5000, System.currentTimeMillis(), predicate, aggregator, sorter,
                        equivalentPredicate);
                long start = System.currentTimeMillis();
                try {
                        while(true) {
                            Utils.sleep(1000);
                            QueryResponse response = queryClient.query(queryRequest);
                            if (response.getEOFFlag()) {
                                System.out.println("EOF.");
                                long end = System.currentTimeMillis();
                                System.out.println(String.format("Query time: %d ms", end - start));
                                break;
                            }
                            System.out.println(response);
                        }
                    } catch (SocketTimeoutException e) {
                        Thread.interrupted();
                    } catch (IOException e) {
                        if (Thread.currentThread().interrupted()) {
                            Thread.interrupted();
                        }
                    } catch (ClassNotFoundException e) {
                        e.printStackTrace();
                    }

            }

//            while (true) {
//
//                final Double xLow = 10.0;
//                final Double xHigh = 15.0;
//                final Double yLow = 40.0;
//                final Double yHigh = 50.0;
//                Intervals intervals = city.getZCodeIntervalsInARectagle(xLow, xHigh, yLow, yHigh);
//
//                DataTuplePredicate predicate = new DataTuplePredicate() {
//                    @Override
//                    public boolean test(DataTuple objects) {
//                        return (double)rawSchema.getValue("lon", objects) >= xLow &&
//                                (double)rawSchema.getValue("lon", objects) <= xHigh &&
//                                (double)rawSchema.getValue("lat", objects) >= yLow &&
//                                (double)rawSchema.getValue("lat", objects) <= yHigh;
//                    }
//                };
//
//
//                for (Interval interval: intervals.intervals) {
//                    QueryRequest queryRequest = new QueryRequest(interval.low, interval.high, System.currentTimeMillis()-6000, System.currentTimeMillis(), predicate);
//                    try {
//                        while(true) {
//                            QueryResponse response = queryClient.query(queryRequest);
//                            if (response.getEOFFlag()) {
//                                System.out.println("EOF.");
//                                break;
//                            }
//                            System.out.println(response);
//                        }
//                    } catch (IOException e) {
//                        e.printStackTrace();
//                    } catch (ClassNotFoundException e) {
//                        e.printStackTrace();
//                    }
//                }
//                System.out.println("A query is fully executed!");
//            }
        });
        queryThread.start();

        Utils.sleep(50000);
        cluster.shutdown();
        System.out.println("Local cluster is shut down!");
        ingestionThread.interrupt();
        clientBatchMode.close();
        queryClient.close();
        queryThread.interrupt();
        System.out.println("Waiting to interrupt!");
//        StormSubmitter.submitTopologyWithProgressBar(args[0], conf, topology);
    }


    static private DataSchema getRawDataSchema() {
        DataSchema rawSchema = new DataSchema();
        rawSchema.addVarcharField("id", 32);
        rawSchema.addVarcharField("veh_no", 10);
        rawSchema.addDoubleField("lon");
        rawSchema.addDoubleField("lat");
        rawSchema.addIntField("car_status");
        rawSchema.addDoubleField("speed");
        rawSchema.addVarcharField("position_type", 10);
        rawSchema.addVarcharField("update_time", 32);
        return rawSchema;
    }

    static private DataSchema getDataSchema() {
        DataSchema schema = new DataSchema();
        schema.addVarcharField("id", 32);
        schema.addVarcharField("veh_no", 10);
        schema.addDoubleField("lon");
        schema.addDoubleField("lat");
        schema.addIntField("car_status");
        schema.addDoubleField("speed");
        schema.addVarcharField("position_type", 10);
        schema.addVarcharField("update_time", 32);
        schema.addIntField("zcode");
        schema.addLongField("timestamp");
        schema.setPrimaryIndexField("zcode");
        return schema;
    }

}
