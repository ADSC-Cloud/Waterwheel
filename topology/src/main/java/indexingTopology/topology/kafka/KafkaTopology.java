package indexingTopology.topology.kafka;

import indexingTopology.api.client.GeoTemporalQueryClient;
import indexingTopology.api.client.GeoTemporalQueryRequest;
import indexingTopology.api.client.QueryResponse;
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
import indexingTopology.bolt.InputStreamKafkaReceiverBolt;
import indexingTopology.bolt.InputStreamKafkaReceiverBoltServer;
import indexingTopology.topology.TopologyGenerator;
import indexingTopology.util.AvailableSocketPool;
import indexingTopology.util.shape.*;
import indexingTopology.util.taxi.Car;
import indexingTopology.util.taxi.City;
import indexingTopology.util.taxi.TrajectoryGenerator;
import indexingTopology.util.taxi.TrajectoryMovingGenerator;
import net.sf.json.JSONObject;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.generated.AlreadyAliveException;
import org.apache.storm.generated.AuthorizationException;
import org.apache.storm.generated.InvalidTopologyException;
import org.apache.storm.generated.StormTopology;
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;

import java.io.IOException;
import java.io.Serializable;
import java.net.SocketTimeoutException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.function.Function;

/**
 * Created by billlin on 2017/11/25
 */
public class KafkaTopology {

    /**
     * general configuration
     */
    @Option(name = "--help", aliases = {"-h"}, usage = "help")
    private boolean Help = false;

//    @Option(name = "--mode", aliases = {"-m"}, usage = "submit|ingest|query")
//    private String Mode = "Not Given";

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
     * query api configuration
     */
    @Option(name = "--query-server-ip", usage = "the query server ip")
    private String QueryServerIp = "localhost";

    @Option(name = "--selectivity", usage = "the selectivity on the key domain")
    private double Selectivity = 1;

    @Option(name = "--temporal", usage = "recent time in seconds of interest")
    private int RecentSecondsOfInterest = 100;

    @Option(name = "--queries", usage = "number of queries to perform")
//    private int NumberOfQueries = Integer.MAX_VALUE;
    private int NumberOfQueries = 20;



    static final double x1 = 80.012928;
    static final double x2 = 90.023983;
    static final double y1 = 70.292677;
    static final double y2 = 80.614865;
    static final int partitions = 128;
    int totalNumber = 0;
    Producer<String, String> producer = null;
    AvailableSocketPool socketPool = new AvailableSocketPool();
    int queryPort = socketPool.getAvailablePort();

    public void excuteQuery(){
        DataSchema schema = getDataSchema();
        String searchTest = "{\"type\":\"rectangle\",\"leftTop\":\"80,80\",\"rightBottom\":\"90,70\",\"geoStr\":null,\"lon\":null,\"lat\":null,\"radius\":null}";
        String searchTest2 = "{\"type\":\"circle\",\"leftTop\":null,\"rightBottom\":null,\"geoStr\":null,\"lon\":85,\"lat\":75,\"radius\":5}";
        String searchTest3 = "{\"type\":\"polygon\",\"leftTop\":null,\"rightBottom\":null,\"geoStr\":[\"80  75\",\"85  80\",\"90  75\",\"85  70\"],\"lon\":null,\"lat\":null,\"radius\":null}";
        JSONObject jsonObject = JSONObject.fromObject(searchTest);
        ShapeChecking shapeChecking = new ShapeChecking(jsonObject);
        ArrayList arrayList= shapeChecking.split();
        Point leftTop;
        Point rightBottom;
        if(shapeChecking.getError() != null){
            System.out.println("Error! ErrorCode :  " + shapeChecking.getError());
            return;
        }
        DataTuplePredicate predicate;
        if(jsonObject.get("type").equals("rectangle")){
            Rectangle rectangle = new Rectangle(new Point(Double.valueOf(String.valueOf(arrayList.get(0))),(Double.valueOf(String.valueOf(arrayList.get(1))))),new Point(Double.valueOf(String.valueOf(arrayList.get(2))),(Double.valueOf(String.valueOf(arrayList.get(3))))));
            predicate = t -> rectangle.checkIn(new Point((Double)schema.getValue("lon", t),(Double)schema.getValue("lat", t)));
            leftTop = new Point(rectangle.getExternalRectangle().getLeftTopX(), rectangle.getExternalRectangle().getLeftTopY());
            rightBottom = new Point(rectangle.getExternalRectangle().getRightBottomX(), rectangle.getExternalRectangle().getRightBottomY());
        }
        else if(jsonObject.get("type").equals("circle")){
            Circle circle = new Circle((Double.valueOf(String.valueOf(arrayList.get(0)))),(Double.valueOf(String.valueOf(arrayList.get(1)))),(Double.valueOf(String.valueOf(arrayList.get(2)))));
            predicate = t -> circle.checkIn(new Point((Double)schema.getValue("lon", t),(Double)schema.getValue("lat", t)));
            leftTop = new Point(circle.getExternalRectangle().getLeftTopX(), circle.getExternalRectangle().getLeftTopY());
            rightBottom = new Point(circle.getExternalRectangle().getRightBottomX(), circle.getExternalRectangle().getRightBottomY());
        }
        else if(jsonObject.get("type").equals("polygon")){
            Polygon.Builder builder = Polygon.Builder();
            for(int i = 0;i < arrayList.size();i++){
                builder.addVertex(new Point((Double.valueOf(String.valueOf( arrayList.get(i++)))),(Double.valueOf(String.valueOf( arrayList.get(i))))));
            }
            Polygon polygon = builder.build();
            predicate = t -> polygon.checkIn(new Point((Double)schema.getValue("lon", t),(Double)schema.getValue("lat", t)));
            leftTop = new Point(polygon.getExternalRectangle().getLeftTopX(), polygon.getExternalRectangle().getLeftTopY());
            rightBottom = new Point(polygon.getExternalRectangle().getRightBottomX(), polygon.getExternalRectangle().getRightBottomY());

        }
        else{
            predicate = null;
            throw new IllegalArgumentException("Illegal arguments of checking shape.");
        }
        double selectivityOnOneDimension = Math.sqrt(Selectivity);
        GeoTemporalQueryClient queryClient = new GeoTemporalQueryClient(QueryServerIp, queryPort);
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

                final int id = new Random().nextInt(100000);
                final String idString = "" + id;
                DataTupleEquivalentPredicateHint equivalentPredicateHint = new DataTupleEquivalentPredicateHint("id", idString);


                double x = x1 + (x2 - x1) * (1 - selectivityOnOneDimension) * random.nextDouble();
                double y = y1 + (y2 - y1) * (1 - selectivityOnOneDimension) * random.nextDouble();

                final double xLow = leftTop.x;
                final double xHigh =rightBottom.x;
                final double yLow = rightBottom.y;
                final double yHigh =leftTop.y;

                GeoTemporalQueryRequest queryRequest = new GeoTemporalQueryRequest<>(xLow, xHigh, yLow, yHigh,
                        System.currentTimeMillis() - RecentSecondsOfInterest * 1000,
                        System.currentTimeMillis(), predicate, null, null, null, null);
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
                    System.out.println("datatuples : " + response.dataTuples.size());
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

                // interval time
                try {
                    Thread.sleep(2000);
                } catch (InterruptedException e) {
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

    public void  submitTopology(){
        DataSchema rawSchema = getRawDataSchema();
        DataSchema schema = getDataSchema();
        TopologyConfig config = new TopologyConfig();
        final boolean enableLoadBalance = false;

        City city = new City(x1, x2, y1, y2, partitions);

        Integer lowerBound = 0;
//        Integer upperBound = city.getMaxZCode();
        Integer upperBound = 5000;

        if (! confFile.equals("none")) {
            config.override(confFile);
            System.out.println("Topology is overridden by " + confFile);
            System.out.println(config.getCriticalSettings());
        } else {
            System.out.println("conf.yaml is not specified, using default instead.");
        }
        //change this one
//        InputStreamReceiverBolt dataSource = new InputStreamReceiverBoltServer(rawSchema, 10000, config);





        int total = 100;
        Thread emittingThread;
        long start = System.currentTimeMillis();
        System.out.println("Kafka Producer send msg start,total msgs:"+total);

        // set up the producer
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("group.id", 0);
        props.put("acks", "all");
        props.put("retries", "0");
        props.put("batch.size", 16384);
        props.put("auto.commit.interval.ms", "1000");
        props.put("buffer.memory", 33554432);
        props.put("key.serializer", StringSerializer.class.getName());
        props.put("value.serializer", StringSerializer.class.getName());
        producer = new KafkaProducer<String, String>(props);
        TrajectoryGenerator generator = new TrajectoryMovingGenerator(x1, x2, y1, y2, 100000, 45.0);

//            producer = new KafkaProducer<>(props);
        emittingThread = new Thread(() -> {
            while (!Thread.currentThread().isInterrupted()) {
                try {
                    for (int i = 0; i < total; i++) {
                        Car car = generator.generate();
                        totalNumber++;
                        Long timestamp = System.currentTimeMillis();
                        String locationtime = String.valueOf(new Date(timestamp));
                        Double lon = Math.random() * 100;
                        Double lat = Math.random() * 100;
                        final int id = new Random().nextInt(100);
                        final String idString = "" + id;
//                          int id = (int) (Math.random() * 100);
//                       "{"devbtype":3,"devid":"asd","timestamp":10086,"id":1}"
                        this.producer.send(new ProducerRecord<String, String>("consumer",
                                String.valueOf(i),
//                                \"timestamp\":"+ timestamp +",
                                "{\"lon\":"+ lon + ",\"lat\":" + lat + ",\"devbtype\":"+ totalNumber +",\"devid\":\"asd\",\"id\":"+ idString +"}"));
//                        this.producer.send(new ProducerRecord<String, String>("consumer",
//                                String.valueOf(i), "{\"employees\":[{\"firstName\":\"John\",\"lastName\":\"Doe\"},{\"firstName\":\"Anna\",\"lastName\":\"Smith\"},{\"firstName\":\"Peter\",\"lastName\":\"Jones\"}]}"));
                        //                        String.format("{\"type\":\"test\", \"t\":%d, \"k\":%d}", System.currentTimeMillis(), i)));

                        // every so often send to a different topic
                        //                if (i % 1000 == 0) {
                        //                    producer.send(new ProducerRecord<String, String>("test", String.format("{\"type\":\"marker\", \"t\":%d, \"k\":%d}", System.currentTimeMillis(), i)));
                        //                    producer.send(new ProducerRecord<String, String>("hello", String.format("{\"type\":\"marker\", \"t\":%d, \"k\":%d}", System.currentTimeMillis(), i)));

                        this.producer.flush();
//                        System.out.println("Sent msg number " + totalNumber);
                        //                }
                    }
                    //            producer.close();
                    System.out.println("Kafka Producer send msg over,cost time:" + (System.currentTimeMillis() - start) + "ms");
                    Thread.sleep(1000);
                } catch (Exception e) {
                    e.printStackTrace();
                }

            }
        });
        emittingThread.start();



        InputStreamKafkaReceiverBolt dataSource = new InputStreamKafkaReceiverBoltServer(rawSchema,10000,config);

//        QueryCoordinatorBolt<Integer> coordinator = new QueryCoordinatorWithQueryReceiverServerBolt<>(minIndex, maxIndex, queryPort,
//                config, schema);
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

        TopologyGenerator<Integer> topologyGenerator = new TopologyGenerator<>();
        topologyGenerator.setNumberOfNodes(NumberOfNodes);

        StormTopology topology = topologyGenerator.generateIndexingTopology(schema, lowerBound, upperBound, enableLoadBalance, dataSource,
                coordinator,dataTupleMapper, bloomFilterColumns , config);

        Config conf = new Config();
        conf.setDebug(false);
        conf.setNumWorkers(NumberOfNodes);

        conf.put(Config.WORKER_CHILDOPTS, "-Xmx1024m");
        conf.put(Config.WORKER_HEAP_MEMORY_MB, 1024);
        conf.put(Config.STORM_MESSAGING_NETTY_MAX_SLEEP_MS, 1);

        // use ResourceAwareScheduler with some magic configurations to ensure that QueryCoordinator and Sink
        // are executed on the nimbus node.
        conf.setTopologyStrategy(org.apache.storm.scheduler.resource.strategies.scheduling.DefaultResourceAwareStrategy.class);


        LocalCluster localCluster = new LocalCluster();
        localCluster.submitTopology(TopologyName, conf, topology);

    }
    public static void main(String[] args) throws InvalidTopologyException, AuthorizationException, AlreadyAliveException {

        KafkaTopology kafkaTopology = new KafkaTopology();

        CmdLineParser parser = new CmdLineParser(kafkaTopology);

        try {
            parser.parseArgument(args);
        } catch (CmdLineException e) {
            e.printStackTrace();
            parser.printUsage(System.out);
        }

        if (kafkaTopology.Help) {
            parser.printUsage(System.out);
            return;
        }

        kafkaTopology.submitTopology();
        try {
            Thread.sleep(10000);
            kafkaTopology.excuteQuery();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

    }

    static private DataSchema getRawDataSchema() {
        DataSchema rawSchema = new DataSchema();
        rawSchema.addDoubleField("lon");
        rawSchema.addDoubleField("lat");
        rawSchema.addIntField("devbtype");
        rawSchema.addVarcharField("devid", 8);
        rawSchema.addVarcharField("id", 32);
        return rawSchema;
    }

    static private DataSchema getDataSchema() {
        DataSchema schema = new DataSchema();
        schema.addDoubleField("lon");
        schema.addDoubleField("lat");
        schema.addIntField("devbtype");
        schema.addVarcharField("devid", 8);
        schema.addVarcharField("id", 32);
        schema.addIntField("zcode");
        schema.addLongField("timestamp");
        schema.setPrimaryIndexField("zcode");

        return schema;
    }
}
