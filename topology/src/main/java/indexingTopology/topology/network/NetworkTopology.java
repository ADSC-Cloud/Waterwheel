package indexingTopology.topology.network;

import indexingTopology.api.client.*;
import indexingTopology.bolt.*;
import indexingTopology.common.data.DataSchema;
import indexingTopology.common.logics.DataTupleEquivalentPredicateHint;
import indexingTopology.config.TopologyConfig;
import indexingTopology.topology.TopologyGenerator;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.AlreadyAliveException;
import org.apache.storm.generated.AuthorizationException;
import org.apache.storm.generated.InvalidTopologyException;
import org.apache.storm.generated.StormTopology;
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;

import java.io.IOException;
import java.net.SocketTimeoutException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.List;
import java.util.Random;

/**
 * Created by acelzj on 6/19/17.
 */
public class NetworkTopology {

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
     * ingest api configuration
     */
    @Option(name = "--ingest-server-ip", usage = "the ingestion server ip")
    private String IngestServerIp = "localhost";

    @Option(name = "--ingest-rate-limit", aliases = {"-r"}, usage = "max ingestion rate")
    private int MaxIngestRate = Integer.MAX_VALUE;

    /**
     * query api configuration
     */
    @Option(name = "--query-server-ip", usage = "the query server ip")
    private String QueryServerIp = "localhost";

    @Option(name = "--selectivity", usage = "the selectivity on the key domain")
    private double Selectivity = 1;

    @Option(name = "--temporal", usage = "recent time in seconds of interest")
    private int RecentSecondsOfInterest = 5;

    @Option(name = "--queries", usage = "number of queries to perform")
    private int NumberOfQueries = Integer.MAX_VALUE;


    static final int lowerBound = -2147341772;
    static final int upperBound = 2118413319;

    public void executeQuery() {

        double selectivityOnOneDimension = Math.sqrt(Selectivity);
        DataSchema schema = getDataSchema();
        NetworkTemporalQueryClient queryClient = new NetworkTemporalQueryClient(QueryServerIp, 10001);
        Thread queryThread = new Thread(() -> {
            try {
                queryClient.connectWithTimeout(10000);
            } catch (IOException e) {
                e.printStackTrace();
                return;
            }
            Random random = new Random();

            int executed = 0;
            long totalQueryTime = 0;

            while (true) {

                double destIp = lowerBound + (upperBound - lowerBound) * (1 - selectivityOnOneDimension) * random.nextDouble();
//                double y = y1 + (y2 - y1) * (1 - selectivityOnOneDimension) * random.nextDouble();

                final double destIpLow = destIp;
                final double destIpHigh = destIp + selectivityOnOneDimension * (upperBound - lowerBound);
//                final double yLow = y;
//                final double yHigh = y + selectivityOnOneDimension * (y2 - y1);

//                DataTuplePredicate predicate = t ->
//                                 (double) schema.getValue("lon", t) >= destIpLow &&
//                                (double) schema.getValue("lon", t) <= destIpHigh &&
//                                (double) schema.getValue("lat", t) >= yLow &&
//                                (double) schema.getValue("lat", t) <= yHigh ;

                final int id = new Random().nextInt(100000);
                final String idString = "" + id;
//                DataTuplePredicate predicate = t -> schema.getValue("id", t).equals(Integer.toString(new Random().nextInt(100000)));
//                DataTuplePredicate predicate = t -> schema.getValue("id", t).equals(idString);



//                Aggregator<Integer> aggregator = new Aggregator<>(schema, "id", new AggregateField(new Count(), "*"));
//                Aggregator<Integer> aggregator = null;


//                DataSchema schemaAfterAggregation = aggregator.getOutputDataSchema();
//                DataTupleSorter sorter = (DataTuple o1, DataTuple o2) -> Double.compare((double) schemaAfterAggregation.getValue("count(*)", o1),
//                        (double) schemaAfterAggregation.getValue("count(*)", o2));


                DataTupleEquivalentPredicateHint equivalentPredicateHint = new DataTupleEquivalentPredicateHint("id", idString);

                NetworkTemporalQueryRequest queryRequest = new NetworkTemporalQueryRequest<>((int) destIpLow, (int) destIpHigh,
                        System.currentTimeMillis() - RecentSecondsOfInterest * 1000,
                        System.currentTimeMillis(), null, null, null, null, equivalentPredicateHint);
                long start = System.currentTimeMillis();
                try {
                    DateFormat dateFormat = new SimpleDateFormat("MM-dd HH:mm:ss");
                    Calendar cal = Calendar.getInstance();
                    System.out.println("[" + dateFormat.format(cal.getTime()) + "]: A query will be issued.");
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

//    public void executeIngestion() {
//
//        DataSchema rawSchema = getRawDataSchema();
//        TrajectoryGenerator generator = new TrajectoryMovingGenerator(x1, x2, y1, y2, 100000, 45.0);
//        IngestionClientBatchMode clientBatchMode = new IngestionClientBatchMode(IngestServerIp, 10000,
//                rawSchema, 1024);
//
//        RateTracker rateTracker = new RateTracker(1000,2);
//        FrequencyRestrictor restrictor = new FrequencyRestrictor(MaxIngestRate, 5);
//
//        Thread ingestionThread = new Thread(()->{
//            Random random = new Random();
//
//            try {
//                clientBatchMode.connectWithTimeout(10000);
//            } catch (IOException e) {
//                e.printStackTrace();
//            }
//            while(true) {
//                Car car = generator.generate();
//                DataTuple tuple = new DataTuple();
//                tuple.add(Integer.toString((int)car.id));
//                tuple.add(Integer.toString(random.nextInt()));
//                tuple.add(car.x);
//                tuple.add(car.y);
//                tuple.add(1);
//                tuple.add(55.3);
//                tuple.add("position 1");
//                tuple.add("2015-10-10, 11:12:34");
//                try {
//                    restrictor.getPermission();
//                    clientBatchMode.appendInBatch(tuple);
//                    rateTracker.notify(1);
//                    if(Thread.interrupted()) {
//                        break;
//                    }
//                } catch (IOException e) {
////                    if (clientBatchMode.isClosed()) {
//                    try {
//                        System.out.println("try to reconnect....");
//                        clientBatchMode.connectWithTimeout(10000);
//                        System.out.println("connected.");
//                    } catch (IOException e1) {
//                        e1.printStackTrace();
//                    }
////                    }
//                    e.printStackTrace();
//                    try {
//                        Thread.sleep(1000);
//                    } catch (InterruptedException e1) {
//                        e1.printStackTrace();
//                        Thread.currentThread().interrupt();
//                    }
//                } catch (InterruptedException e) {
//                    e.printStackTrace();
//                }
//
////                try {
//////                    Thread.sleep(1);
////                } catch (InterruptedException e) {
////                    e.printStackTrace();
////                }
//
//            }
//        });
//        ingestionThread.start();
//
//        new Thread(()->{
//            while (true) {
//                try {
//                    Thread.sleep(1000);
//                } catch (InterruptedException e) {
//                    e.printStackTrace();
//                    break;
//                }
//                DateFormat dateFormat = new SimpleDateFormat("MM-dd HH:mm:ss");
//                Calendar cal = Calendar.getInstance();
//                System.out.println("[" + dateFormat.format(cal.getTime()) + "]: " + rateTracker.reportRate() + " tuples/s");
//            }
//        }).start();
//    }

    public void submitTopology() throws InvalidTopologyException, AuthorizationException, AlreadyAliveException {
        DataSchema schema = getDataSchema();

        Integer lowerBound = -2147341772;
        Integer upperBound = 2118413319;

        final boolean enableLoadBalance = false;

        TopologyConfig config = new TopologyConfig();

        InputStreamReceiverBolt dataSource = new NetworkDataSource(schema, config);

        QueryCoordinatorBolt<Integer> queryCoordinatorBolt = new NetworkTemporalQueryCoordinatorWithQueryReceiverServerBolt<>(lowerBound,
                upperBound, 10001, config, schema);

//        DataTupleMapper dataTupleMapper = new DataTupleMapper(rawSchema, (Serializable & Function<DataTuple, DataTuple>) t -> {
//            double lon = (double)schema.getValue("lon", t);
//            double lat = (double)schema.getValue("lat", t);
//            int zcode = city.getZCodeForALocation(lon, lat);
//            t.add(zcode);
//            t.add(System.currentTimeMillis());
//            return t;
//        });

        List<String> bloomFilterColumns = new ArrayList<>();
//        bloomFilterColumns.add("id");

        TopologyGenerator<Integer> topologyGenerator = new TopologyGenerator<>();
        topologyGenerator.setNumberOfNodes(NumberOfNodes);

        StormTopology topology = topologyGenerator.generateIndexingTopology(schema, lowerBound, upperBound,
                enableLoadBalance, dataSource, queryCoordinatorBolt, null, bloomFilterColumns, config);

        Config conf = new Config();
        conf.setDebug(false);
        conf.setNumWorkers(NumberOfNodes);

        conf.put(Config.WORKER_CHILDOPTS, "-Xmx1024m");
        conf.put(Config.WORKER_HEAP_MEMORY_MB, 1024);

        if (LocalMode) {
            LocalCluster localCluster = new LocalCluster();
            localCluster.submitTopology(TopologyName, conf, topology);
        } else {
            StormSubmitter.submitTopology(TopologyName, conf, topology);
        }
    }

    public static void main(String[] args) throws InvalidTopologyException, AuthorizationException, AlreadyAliveException {

        NetworkTopology networkTopology = new NetworkTopology();

        CmdLineParser parser = new CmdLineParser(networkTopology);

        try {
            parser.parseArgument(args);
        } catch (CmdLineException e) {
            e.printStackTrace();
            parser.printUsage(System.out);
        }

        if (networkTopology.Help) {
            parser.printUsage(System.out);
        }

        switch (networkTopology.Mode) {
            case "submit": networkTopology.submitTopology(); break;
//            case "ingest": networkTopology.executeIngestion(); break;
            case "query": networkTopology.executeQuery(); break;
            default: System.out.println("Invalid command!");
        }
//        if (command.equals("submit"))
//            submitTopology();
//        else if (command.equals("ingest"))
//            executeIngestion();
//        else if (command.equals("query"))
//            executeQuery();

    }


    static private DataSchema getRawDataSchema() {
        DataSchema rawSchema = new DataSchema();
//        schema.addDoubleField("id");
        rawSchema.addIntField("sourceIP");
        rawSchema.addIntField("destIP");
        rawSchema.addVarcharField("url", 21);
        return rawSchema;
    }

    static private DataSchema getDataSchema() {
        DataSchema schema = new DataSchema();
        schema.addIntField("sourceIP");
        schema.addIntField("destIP");
        schema.addVarcharField("url", 21);
        schema.addLongField("timestamp");
        schema.setPrimaryIndexField("destIP");
        return schema;
    }
}
