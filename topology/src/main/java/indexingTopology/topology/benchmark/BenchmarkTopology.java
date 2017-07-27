package indexingTopology.topology.benchmark;

import indexingTopology.api.client.*;
import indexingTopology.bolt.*;
import indexingTopology.common.aggregator.AggregateField;
import indexingTopology.common.aggregator.Aggregator;
import indexingTopology.common.aggregator.Count;
import indexingTopology.common.data.DataSchema;
import indexingTopology.common.data.DataTuple;
import indexingTopology.common.logics.DataTupleEquivalentPredicateHint;
import indexingTopology.common.logics.DataTupleMapper;
import indexingTopology.common.logics.DataTuplePredicate;
import indexingTopology.config.TopologyConfig;
import indexingTopology.topology.TopologyGenerator;
import indexingTopology.util.FrequencyRestrictor;
import indexingTopology.util.taxi.Car;
import indexingTopology.util.taxi.City;
import indexingTopology.util.taxi.TrajectoryGenerator;
import indexingTopology.util.taxi.TrajectoryMovingGenerator;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.AlreadyAliveException;
import org.apache.storm.generated.AuthorizationException;
import org.apache.storm.generated.InvalidTopologyException;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.metric.internal.RateTracker;
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;

import java.io.IOException;
import java.io.Serializable;
import java.math.BigInteger;
import java.net.SocketTimeoutException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.time.temporal.TemporalQuery;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.List;
import java.util.Random;
import java.util.function.Function;


public class BenchmarkTopology {

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

    @Option(name = "--config-file", aliases = {"-f"}, usage = "conf.yaml to override default configs")
    private String confFile = "none";

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

    static final int keys = 1024 * 1024;
    static final int x1 = 0, x2 = keys - 1;
    static final int payloadSize = 100;

    public void executeQuery() {

        DataSchema schema = getDataSchema();
        QueryClient queryClient = new QueryClient(QueryServerIp, 10001);
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

                int x = (int)(x1 + (x2 - x1) * (1 - Selectivity) * random.nextDouble());

                final int xLow = x;
                final int xHigh = (int) (x + Selectivity * (x2 - x1));

//                DataTuplePredicate predicate = t ->
//                                 (double) schema.getValue("lon", t) >= xLow &&
//                                (double) schema.getValue("lon", t) <= xHigh &&
//                                (double) schema.getValue("lat", t) >= yLow &&
//                                (double) schema.getValue("lat", t) <= yHigh ;

                final int id = new Random().nextInt(keys);
                final String idString = "" + id;
//                DataTuplePredicate predicate = t -> schema.getValue("id", t).equals(Integer.toString(new Random().nextInt(100000)));
                DataTuplePredicate predicate = null;
//                DataTuplePredicate predicate = t -> schema.getValue("id", t).equals(idString);



                Aggregator<Integer> aggregator = new Aggregator<>(schema, "id", new AggregateField(new Count(), "*"));
//                Aggregator<Integer> aggregator = null;


//                DataSchema schemaAfterAggregation = aggregator.getOutputDataSchema();
//                DataTupleSorter sorter = (DataTuple o1, DataTuple o2) -> Double.compare((double) schemaAfterAggregation.getValue("count(*)", o1),
//                        (double) schemaAfterAggregation.getValue("count(*)", o2));


//                DataTupleEquivalentPredicateHint equivalentPredicateHint = new DataTupleEquivalentPredicateHint("id", idString);
                DataTupleEquivalentPredicateHint equivalentPredicateHint = null;

                QueryRequest<Integer> queryRequest = new QueryRequest<>(xLow, xHigh,
                        System.currentTimeMillis() - RecentSecondsOfInterest * 1000,
                        System.currentTimeMillis(), predicate, aggregator, null, equivalentPredicateHint);
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
                    System.out.println(String.format("Query time [%d]: %d ms", executed, end - start));

                    if (++executed >= NumberOfQueries) {
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

        DataSchema schema = getDataSchema();
        IngestionClientBatchMode clientBatchMode = new IngestionClientBatchMode(IngestServerIp, 10000,
                schema, 1024);

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

                DataTuple tuple = new DataTuple();
                tuple.add(random.nextInt(1024));
                tuple.add(random.nextInt(keys));
                tuple.add( new BigInteger(130, random).toString(payloadSize));
                tuple.add(System.currentTimeMillis());
                try {
                    restrictor.getPermission();
                    clientBatchMode.appendInBatch(tuple);
                    rateTracker.notify(1);
                    if(Thread.interrupted()) {
                        break;
                    }
                } catch (IOException e) {
//                    if (clientBatchMode.isClosed()) {
                    try {
                        System.out.println("try to reconnect....");
                        clientBatchMode.connectWithTimeout(10000);
                        System.out.println("connected.");
                    } catch (IOException e1) {
                        e1.printStackTrace();
                    }
//                    }
                    e.printStackTrace();
                    try {
                        Thread.sleep(1000);
                    } catch (InterruptedException e1) {
                        e1.printStackTrace();
                        Thread.currentThread().interrupt();
                    }
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
                DateFormat dateFormat = new SimpleDateFormat("MM-dd HH:mm:ss");
                Calendar cal = Calendar.getInstance();
                System.out.println("[" + dateFormat.format(cal.getTime()) + "]: " + rateTracker.reportRate() + " tuples/s");
            }
        }).start();
    }

    public void submitTopology() throws InvalidTopologyException, AuthorizationException, AlreadyAliveException {
        DataSchema schema = getDataSchema();

        final boolean enableLoadBalance = false;

        TopologyConfig config = new TopologyConfig();


        if (! confFile.equals("none")) {
            config.override(confFile);
            System.out.println("Topology is overridden by " + confFile);
            System.out.println(config.getCriticalSettings());
        } else {
            System.out.println("conf.yaml is not specified, using default instead.");
        }

        InputStreamReceiverBolt dataSource = new InputStreamReceiverBoltServer(schema, 10000, config);

        QueryCoordinatorWithQueryReceiverServerBolt<Integer> queryCoordinatorBolt = new QueryCoordinatorWithQueryReceiverServerBolt<>(x1,
                x2, 10001, config, schema);

        DataTupleMapper dataTupleMapper = null;

        List<String> bloomFilterColumns = new ArrayList<>();
//        bloomFilterColumns.add("id");

        TopologyGenerator<Integer> topologyGenerator = new TopologyGenerator<>();
        topologyGenerator.setNumberOfNodes(NumberOfNodes);

        StormTopology topology = topologyGenerator.generateIndexingTopology(schema, x1, x2,
                enableLoadBalance, dataSource, queryCoordinatorBolt, dataTupleMapper, bloomFilterColumns, config);

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
            System.out.println("Topology is successfully submitted to the cluster!");
            System.out.println(config.getCriticalSettings());
        }
    }

    public static void main(String[] args) throws InvalidTopologyException, AuthorizationException, AlreadyAliveException {

        BenchmarkTopology topology = new BenchmarkTopology();

        CmdLineParser parser = new CmdLineParser(topology);

        try {
            parser.parseArgument(args);
        } catch (CmdLineException e) {
            e.printStackTrace();
            parser.printUsage(System.out);
        }

        if (topology.Help) {
            parser.printUsage(System.out);
            return;
        }

        switch (topology.Mode) {
            case "submit": topology.submitTopology(); break;
            case "ingest": topology.executeIngestion(); break;
            case "query": topology.executeQuery(); break;
            default: System.out.println("Invalid command!");
        }
//        if (command.equals("submit"))
//            submitTopology();
//        else if (command.equals("ingest"))
//            executeIngestion();
//        else if (command.equals("query"))
//            executeQuery();

    }

    static private DataSchema getDataSchema() {
        DataSchema schema = new DataSchema();

        schema.addIntField("id");
        schema.addIntField("key");
        schema.addVarcharField("payload", payloadSize);
        schema.addLongField("timestamp");
        schema.setPrimaryIndexField("key");

        return schema;
    }

}
