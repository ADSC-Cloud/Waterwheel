package indexingTopology.bolt;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import indexingTopology.Config.Config;
import indexingTopology.MetaData.FileMetaData;
import indexingTopology.MetaData.FilePartitionSchemaManager;
import indexingTopology.MetaData.TaskMetaData;
import indexingTopology.MetaData.TaskPartitionSchemaManager;
import indexingTopology.NormalDistributionIndexingTopology;
import indexingTopology.util.Histogram;
import indexingTopology.util.PartitionFunction;
import indexingTopology.util.RepartitionManager;
import javafx.util.Pair;

import java.util.*;

/**
 * Created by acelzj on 12/12/16.
 */
public class MetadataServer extends BaseRichBolt {

    private OutputCollector collector;

    private Map<Integer, Integer> intervalToTaskMapping;

    private Double upperBound;

    private Double lowerBound;

    private List<Integer> indexTasks;

    private FilePartitionSchemaManager filePartitionSchemaManager;

    private PartitionFunction partitionFunction;

    private Map<Integer, Long> indexTaskToTimestampMapping;

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        collector = outputCollector;

        filePartitionSchemaManager = new FilePartitionSchemaManager();

        intervalToTaskMapping = new HashMap<>();


        lowerBound = 0D;

        upperBound = 1000D;
    }

    @Override
    public void execute(Tuple tuple) {
        if (tuple.getSourceStreamId().equals(NormalDistributionIndexingTopology.StatisticsReportStream)) {
            Histogram histogram = (Histogram) tuple.getValue(0);

            lowerBound = tuple.getDouble(1);
            upperBound = tuple.getDouble(2);

            int numberOfBins = indexTasks.size();

            System.out.println("number of bins " + numberOfBins);

            long[] intervalLoads = new long[numberOfBins];

            List<Long> frequencies = histogram.histogramToList();

            for (Integer interval : intervalToTaskMapping.keySet()) {
                intervalLoads[intervalToTaskMapping.get(interval) % numberOfBins] += frequencies.get(interval);
            }

            Double skewnessFactor = getSkewnessFactor(intervalLoads, numberOfBins);

            System.out.println("skewness factor is " + skewnessFactor);

            if (skewnessFactor > 2) {
                RepartitionManager manager = new RepartitionManager(numberOfBins, intervalToTaskMapping,
                        histogram.getHistogram(), getTotalWorkLoad(intervalLoads), indexTasks);
                this.intervalToTaskMapping = manager.getRepartitionPlan();
                partitionFunction = new PartitionFunction(lowerBound, upperBound);
                collector.emit(NormalDistributionIndexingTopology.IntervalPartitionUpdateStream,
                        new Values(this.intervalToTaskMapping, partitionFunction));
            } else {
                partitionFunction = new PartitionFunction(lowerBound, upperBound);
                collector.emit(NormalDistributionIndexingTopology.IntervalPartitionUpdateStream,
                        new Values(new HashMap(), partitionFunction));
            }
        } else if (tuple.getSourceStreamId().equals(NormalDistributionIndexingTopology.FileInformationUpdateStream)) {
            String fileName = tuple.getString(0);
            Pair keyRange = (Pair) tuple.getValue(1);
            Pair timeStampRange = (Pair) tuple.getValue(2);

            filePartitionSchemaManager.add(new FileMetaData(fileName, (Double) keyRange.getKey(),
                    (Double)keyRange.getValue(), (Long) timeStampRange.getKey(), (Long) timeStampRange.getValue()));

            collector.emit(NormalDistributionIndexingTopology.FileInformationUpdateStream,
                    new Values(fileName, keyRange, timeStampRange));
        } else if (tuple.getSourceStreamId().equals(NormalDistributionIndexingTopology.IndexerNumberReportStream)) {
            indexTasks = (List) tuple.getValueByField("numberOfIndexers");

            intervalToTaskMapping = getBalancedPartitionPlan();
            setTimestamps();

            for (Integer taskId : indexTasks) {
                collector.emit(NormalDistributionIndexingTopology.TimeStampUpdateStream,
                        new Values(taskId, indexTaskToTimestampMapping.get(taskId)));
            }

            partitionFunction = new PartitionFunction(lowerBound, upperBound);

            collector.emit(NormalDistributionIndexingTopology.IntervalPartitionUpdateStream,
                    new Values(intervalToTaskMapping, partitionFunction));
        } else if (tuple.getSourceStreamId().equals(NormalDistributionIndexingTopology.TimeStampUpdateStream)) {
            int taskId = tuple.getSourceTask();
            Long timestamp = tuple.getLongByField("timestamp");

            indexTaskToTimestampMapping.put(taskId, timestamp);

            collector.emit(NormalDistributionIndexingTopology.TimeStampUpdateStream, new Values(taskId, timestamp));
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declareStream(NormalDistributionIndexingTopology.IntervalPartitionUpdateStream,
                new Fields("newIntervalPartition", "partitionFunction"));

        outputFieldsDeclarer.declareStream(NormalDistributionIndexingTopology.FileInformationUpdateStream,
                new Fields("fileName", "keyRange", "timeStampRange"));

        outputFieldsDeclarer.declareStream(NormalDistributionIndexingTopology.TimeStampUpdateStream,
                new Fields("taskId", "timestamp"));
    }


    private double getSkewnessFactor(long[] intervalLoads, int numberOfBins) {

//        System.out.println("Before repartition");
//        for (int i = 0; i < intervalLoads.length; ++i) {
//            System.out.println("bin " + i + " : " + intervalLoads[i]);
//        }

        Long sum = getTotalWorkLoad(intervalLoads);
        Long maxWorkload = getMaxWorkLoad(intervalLoads);
        double averageLoad = sum / (double) numberOfBins;

        return maxWorkload / averageLoad;
    }

    private Long getTotalWorkLoad(long[] intervalLoads) {
        Long sum = 0L;

        for (Long load : intervalLoads) {
            sum += load;
        }

        return sum;
    }

    private Long getMaxWorkLoad(long[] intervalLoads) {
        Long maxWorkLoad = Long.MIN_VALUE;

        for (Long load : intervalLoads) {
            maxWorkLoad = ((load > maxWorkLoad) ? load : maxWorkLoad);
        }

        return maxWorkLoad;
    }

    private Map<Integer, Integer> getBalancedPartitionPlan() {
        int numberOfTasks = indexTasks.size();
        Integer distance = (int) ((upperBound - lowerBound) / numberOfTasks);
        Integer miniDistance = (int) ((upperBound - lowerBound) / Config.NUMBER_OF_INTERVALS);
        Double keyRangeUpperBound = lowerBound + distance;
        Double bound = lowerBound + miniDistance;
        Map<Integer, Integer> intervalToTaskMapping = new HashMap<>();
        int index = 0;
        for (int i = 0; i < Config.NUMBER_OF_INTERVALS; ++i) {
            intervalToTaskMapping.put(i, indexTasks.get(index));
            bound += miniDistance;
            if (bound > keyRangeUpperBound) {
                keyRangeUpperBound = keyRangeUpperBound + distance;
                ++index;
            }
        }
//        System.out.println("balanced partition has been finished!");
//        System.out.println(intervalToTaskMapping);
        return intervalToTaskMapping;
    }

    private void setTimestamps() {

        indexTaskToTimestampMapping = new HashMap<>();

        for (Integer taskId : indexTasks) {
            indexTaskToTimestampMapping.put(taskId, System.currentTimeMillis());
        }
    }
}
