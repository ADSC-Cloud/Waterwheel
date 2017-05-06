package indexingTopology;

import indexingTopology.bolt.*;
import indexingTopology.client.IngestionClientBatchMode;
import indexingTopology.client.QueryClient;
import indexingTopology.client.QueryRequest;
import indexingTopology.client.QueryResponse;
import indexingTopology.data.DataSchema;
import indexingTopology.data.DataTuple;
import indexingTopology.util.DataTupleMapper;
import indexingTopology.util.TopologyGenerator;
import indexingTopology.util.texi.Car;
import indexingTopology.util.texi.City;
import indexingTopology.util.texi.TrajectoryGenerator;
import indexingTopology.util.texi.TrajectoryUniformGenerator;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.topology.TopologyBuilder;

import java.io.IOException;
import java.io.Serializable;
import java.util.Random;
import java.util.function.Function;

/**
 * Created by acelzj on 11/15/16.
 */
public class KingBaseTopology {

    public static void main(String[] args) throws Exception {

        final int payloadSize = 1;
        DataSchema rawSchema = new DataSchema();
        rawSchema.addVarcharField("id", 32);
        rawSchema.addVarcharField("veh_no", 10);
        rawSchema.addDoubleField("lon");
        rawSchema.addDoubleField("lat");
        rawSchema.addIntField("car_status");
        rawSchema.addDoubleField("speed");
        rawSchema.addVarcharField("position_type", 10);
        rawSchema.addVarcharField("update_time", 32);



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




        final double x1 = 0;
        final double x2 = 1000;
        final double y1 = 0;
        final double y2 = 500;
        final int partitions = 128;

        TrajectoryGenerator generator = new TrajectoryUniformGenerator(10000, x1, x2, y1, y2);
        City city = new City(x1, x2, y1, y2, partitions);

        Integer lowerBound = 0;
        Integer upperBound = city.getMaxZCode();

        final boolean enableLoadBalance = false;

        InputStreamReceiver dataSource = new InputStreamReceiverServer(rawSchema, 10000);
        QueryCoordinator<Integer> queryCoordinator = new QueryCoordinatorWithQueryReceiverServer<>(lowerBound, upperBound, 10001);

        TopologyGenerator<Integer> topologyGenerator = new TopologyGenerator<>();

        DataTupleMapper dataTupleMapper = new DataTupleMapper(rawSchema, (Serializable & Function<DataTuple, DataTuple>) t -> {
            double lon = (double)schema.getValue("lon", t);
            double lat = (double)schema.getValue("lat", t);
            int zcode = city.getZCodeForALocation(lon, lat);
            t.add(zcode);
            t.add(System.currentTimeMillis());
            return t;
        });

        StormTopology topology = topologyGenerator.generateIndexingTopology(schema, lowerBound, upperBound, enableLoadBalance, dataSource, queryCoordinator, dataTupleMapper);

        Config conf = new Config();
        conf.setDebug(false);
        conf.setNumWorkers(1);

        conf.put(Config.WORKER_CHILDOPTS, "-Xmx2048m");
        conf.put(Config.WORKER_HEAP_MEMORY_MB, 2048);


        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology("T0", conf, topology);

        new Thread(()->{
            Random random = new Random();
            IngestionClientBatchMode clientBatchMode = new IngestionClientBatchMode("localhost", 10000,
                    rawSchema, 1024);
            try {
                clientBatchMode.connectWithTimeout(10000);
            } catch (IOException e) {
                e.printStackTrace();
            }
            while(true) {
                Car car = generator.generate();
                DataTuple tuple = new DataTuple();
                tuple.add(Integer.toString(random.nextInt()));
                tuple.add(Integer.toString(random.nextInt()));
                tuple.add(car.x);
                tuple.add(car.y);
                tuple.add(1);
                tuple.add(55.3);
                tuple.add("position 1");
                tuple.add("2015-10-10, 11:12:34");
                try {
                    clientBatchMode.appendInBatch(tuple);
                } catch (IOException e) {
                    e.printStackTrace();
                } catch (ClassNotFoundException e) {
                    e.printStackTrace();
                }

//                try {
////                    Thread.sleep(1);
//                } catch (InterruptedException e) {
//                    e.printStackTrace();
//                }

            }
        }).start();

        new Thread(() -> {
            QueryClient queryClient = new QueryClient("localhost", 10001);
            try {
                queryClient.connectWithTimeout(10000);
            } catch (IOException e) {
                e.printStackTrace();
            }

            while (true) {
                QueryRequest queryRequest  = new QueryRequest(10,15, System.currentTimeMillis()-5000, System.currentTimeMillis());
                QueryResponse response;
                try {
                    while(true) {
                        response = queryClient.query(queryRequest);
                        if (response.getEOFFlag()) {
                            System.out.println("EOF.");
                            break;
                        }
                        System.out.println(response);
                    }
                } catch (IOException e) {
                    e.printStackTrace();
                } catch (ClassNotFoundException e) {
                    e.printStackTrace();
                }

            }
        }).start();

//        StormSubmitter.submitTopologyWithProgressBar(args[0], conf, topology);
    }


}
