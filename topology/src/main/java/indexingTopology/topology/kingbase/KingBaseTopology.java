package indexingTopology.topology.kingbase;

import indexingTopology.bolt.*;
import indexingTopology.common.aggregator.AggregateField;
import indexingTopology.common.aggregator.Aggregator;
import indexingTopology.common.aggregator.Count;
import indexingTopology.api.client.GeoTemporalQueryClient;
import indexingTopology.api.client.GeoTemporalQueryRequest;
import indexingTopology.api.client.IngestionClientBatchMode;
import indexingTopology.api.client.QueryResponse;
import indexingTopology.common.logics.DataTupleEquivalentPredicateHint;
import indexingTopology.common.logics.DataTupleMapper;
import indexingTopology.common.logics.DataTuplePredicate;
import indexingTopology.config.TopologyConfig;
import indexingTopology.common.data.DataSchema;
import indexingTopology.common.data.DataTuple;
import indexingTopology.topology.TopologyGenerator;
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

import java.io.IOException;
import java.io.Serializable;
import java.net.SocketTimeoutException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.List;
import java.util.Random;
import java.util.function.Function;
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;


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

    @Option(name = "--config-file", aliases = {"-f"}, usage = "conf.yaml to override default configs")
    private String confFile = "conf/conf.yaml";

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
            Random random = new Random();

            int executed = 0;
            long totalQueryTime = 0;

            while (true) {

                try {
                    queryClient.connectWithTimeout(10000);
                } catch (IOException e) {
                    e.printStackTrace();
                    return;
                }
                double x = x1 + (x2 - x1) * (1 - selectivityOnOneDimension) * random.nextDouble();
                double y = y1 + (y2 - y1) * (1 - selectivityOnOneDimension) * random.nextDouble();

                final double xLow = x;
                final double xHigh = x + selectivityOnOneDimension * (x2 - x1);
                final double yLow = y;
                final double yHigh = y + selectivityOnOneDimension * (y2 - y1);

//                DataTuplePredicate predicate = t ->
//                                 (double) schema.getValue("lon", t) >= xLow &&
//                                (double) schema.getValue("lon", t) <= xHigh &&
//                                (double) schema.getValue("lat", t) >= yLow &&
//                                (double) schema.getValue("lat", t) <= yHigh ;

                final int id = new Random().nextInt(100000);
                final String idString = "" + id;
//                DataTuplePredicate predicate = t -> schema.getValue("id", t).equals(Integer.toString(new Random().nextInt(100000)));
                DataTuplePredicate predicate = t -> schema.getValue("id", t).equals(idString);



                Aggregator<Integer> aggregator = new Aggregator<>(schema, "id", new AggregateField(new Count(), "*"));
//                Aggregator<Integer> aggregator = null;


//                DataSchema schemaAfterAggregation = aggregator.getOutputDataSchema();
//                DataTupleSorter sorter = (DataTuple o1, DataTuple o2) -> Double.compare((double) schemaAfterAggregation.getValue("count(*)", o1),
//                        (double) schemaAfterAggregation.getValue("count(*)", o2));


                DataTupleEquivalentPredicateHint equivalentPredicateHint = new DataTupleEquivalentPredicateHint("id", idString);

                GeoTemporalQueryRequest queryRequest = new GeoTemporalQueryRequest<>(xLow, xHigh, yLow, yHigh,
                        System.currentTimeMillis() - RecentSecondsOfInterest * 1000,
                        System.currentTimeMillis(), null, null, aggregator, null,null);
                long start = System.currentTimeMillis();
                try {
                    DateFormat dateFormat = new SimpleDateFormat("MM-dd HH:mm:ss");
                    Calendar cal = Calendar.getInstance();
                    System.out.println("[" + dateFormat.format(cal.getTime()) + "]: A query will be issued.");
                    QueryResponse response = queryClient.query(queryRequest);
                    System.out.println("A query finished.");
                    long end = System.currentTimeMillis();
                    totalQueryTime += end - start;
                    DataSchema outputSchema = response.getSchema();
                    System.out.println(outputSchema.getFieldNames());
                    List<DataTuple> tuples = response.getTuples();
                    for (int i = 0; i < tuples.size(); i++) {
                        System.out.println(tuples.get(i).toValues());
                    }
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
                try {
                    queryClient.close();
                } catch (IOException e) {
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
                rawSchema, 1024);

        RateTracker rateTracker = new RateTracker(1000,2);
        FrequencyRestrictor restrictor = new FrequencyRestrictor(MaxIngestRate, 5);

        Thread ingestionThread = new Thread(()->{
            Random random = new Random();

            try {
                clientBatchMode.connectWithTimeout(10000);
            } catch (IOException e) {
                e.printStackTrace();
            }

            DateFormat dateFormat = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");

            while(true) {
                Car car = generator.generate();
                DataTuple tuple = new DataTuple();
                tuple.add(Integer.toString((int)car.id));
                tuple.add("" + (char)((int)'A'+Math.random()*((int)'Z'-(int)'A'+1))
                        + (char)((int)'A'+Math.random()*((int)'Z'-(int)'A'+1))
                        + Integer.toString(Math.abs(random.nextInt()) + 1000000).substring(0, 5));
                tuple.add(car.x);
                tuple.add(car.y);
                tuple.add((int)(Math.random() * 3));
                tuple.add(Math.random() * 70.0);
                tuple.add(Integer.toString((int)(Math.random() * 15)));
                Calendar cal = Calendar.getInstance();
                tuple.add(dateFormat.format(cal.getTime()));
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
        DataSchema rawSchema = getRawDataSchema();
        DataSchema schema = getDataSchema();
        City city = new City(x1, x2, y1, y2, partitions);

        Integer lowerBound = 0;
        Integer upperBound = city.getMaxZCode();

        final boolean enableLoadBalance = false;

        TopologyConfig config = new TopologyConfig();


        if (! confFile.equals("none")) {
            config.override(confFile);
            System.out.println("Topology is overridden by " + confFile);
            System.out.println(config.getCriticalSettings());
        } else {
            System.out.println("conf.yaml is not specified, using default instead.");
        }

        InputStreamReceiverBolt dataSource = new InputStreamReceiverBoltServer(rawSchema, 10000, config);

        QueryCoordinatorBolt<Integer> queryCoordinatorBolt = new GeoTemporalQueryCoordinatorBoltBolt<>(lowerBound,
                upperBound, 10001, city, config, schema);

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
                enableLoadBalance, dataSource, queryCoordinatorBolt, dataTupleMapper, bloomFilterColumns, config);

        Config conf = new Config();
        conf.setDebug(false);
        conf.setNumWorkers(NumberOfNodes);

        conf.put(Config.WORKER_CHILDOPTS, "-Xmx1024m");
        conf.put(Config.WORKER_HEAP_MEMORY_MB, 1024);
        conf.put(Config.STORM_MESSAGING_NETTY_MAX_SLEEP_MS, 1);
        conf.setTopologyStrategy(waterwheel.scheduler.FFDStrategyByCPU.class);

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
            return;
        }

        switch (kingBaseTopology.Mode) {
            case "submit": kingBaseTopology.submitTopology(); break;
            case "ingest": kingBaseTopology.executeIngestion(); break;
            case "query": kingBaseTopology.executeQuery(); break;
            default: System.out.println("Invalid command!");
        }
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
        schema.setTemporalField("timestamp");
        schema.setPrimaryIndexField("zcode");
        return schema;
    }

}
