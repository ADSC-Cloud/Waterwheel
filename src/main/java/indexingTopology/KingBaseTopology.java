package indexingTopology;

import indexingTopology.bolt.*;
import indexingTopology.data.DataSchema;
import indexingTopology.util.DataTupleMapper;
import indexingTopology.util.TopologyGenerator;
import indexingTopology.util.texi.City;
import indexingTopology.util.texi.TrajectoryGenerator;
import indexingTopology.util.texi.TrajectoryUniformGenerator;
import org.apache.storm.Config;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.topology.TopologyBuilder;

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
        final int partitions = 100;

        TrajectoryGenerator generator = new TrajectoryUniformGenerator(10000, x1, x2, y1, y2);
        City city = new City(x1, x2, y1, y2, partitions);

        Double sigma = 100000.0;
        Double mean = 5000.0;
        Double lowerBound = 0.0;
        Double upperBound = 2 * mean;

        final boolean enableLoadBalance = false;

        InputStreamReceiver dataSource = new InputStreamReceiverServer(schema, 10000);
        QueryCoordinator<Double> queryCoordinator = new QueryCoordinatorWithQueryGenerator<>(lowerBound, upperBound);

        TopologyGenerator<Double> topologyGenerator = new TopologyGenerator<>();

        DataTupleMapper dataTupleMapper = new DataTupleMapper(schema, t -> t);

        StormTopology topology = topologyGenerator.generateIndexingTopology(schema, lowerBound, upperBound, enableLoadBalance, dataSource, queryCoordinator, dataTupleMapper);

        Config conf = new Config();
        conf.setDebug(false);
        conf.setNumWorkers(1);

        conf.put(Config.WORKER_CHILDOPTS, "-Xmx2048m");
        conf.put(Config.WORKER_HEAP_MEMORY_MB, 2048);

        StormSubmitter.submitTopologyWithProgressBar(args[0], conf, topology);
    }


}
