package indexingTopology.bolt;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import indexingTopology.MetaData.FileMetaData;
import indexingTopology.MetaData.FilePartitionSchemaManager;
import indexingTopology.NormalDistributionIndexingTopology;
import indexingTopology.Streams.Streams;
import indexingTopology.util.BalancedPartition;
import indexingTopology.util.Histogram;
import indexingTopology.util.RepartitionManager;
import javafx.util.Pair;

import java.util.*;

/**
 * Created by acelzj on 12/12/16.
 */
public class MetadataServer extends BaseRichBolt {

    private OutputCollector collector;

    private Map<Integer, Integer> intervalToPartitionMapping;

    private Double upperBound;

    private Double lowerBound;

    private FilePartitionSchemaManager filePartitionSchemaManager;

    private Map<Integer, Long> indexTaskToTimestampMapping;

    private List<Integer> indexTasks;

    private BalancedPartition balancedPartition;

    private int numberOfDispatchers;

    private int numberOfPartitions;

    private int numberOfStaticsReceived;

    private long[] partitionLoads = null;

    private Histogram histogram;

    private Thread staticsRequestSendingThread;


    public MetadataServer(Double lowerBound, Double upperBound) {
        this.lowerBound = lowerBound;
        this.upperBound = upperBound;
    }

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        collector = outputCollector;

        filePartitionSchemaManager = new FilePartitionSchemaManager();

        intervalToPartitionMapping = new HashMap<>();

        numberOfDispatchers = topologyContext.getComponentTasks("DispatcherBolt").size();

        indexTasks = topologyContext.getComponentTasks("IndexerBolt");

        numberOfPartitions = indexTasks.size();

        balancedPartition = new BalancedPartition(numberOfPartitions, lowerBound, upperBound);

        numberOfStaticsReceived = 0;

        indexTaskToTimestampMapping = new HashMap<>();

        histogram = new Histogram();

        staticsRequestSendingThread = new Thread(new StatisticsRequestSendingRunnable());
        staticsRequestSendingThread.start();
    }

    @Override
    public void execute(Tuple tuple) {

        if (tuple.getSourceStreamId().equals(Streams.StatisticsReportStream)) {

            System.out.println(tuple.getSourceStreamId());

            Histogram histogram = (Histogram) tuple.getValue(0);

            if (numberOfStaticsReceived < numberOfDispatchers) {
                partitionLoads = new long[numberOfPartitions];
                this.histogram.merge(histogram);
                ++numberOfStaticsReceived;
                if (numberOfStaticsReceived == numberOfDispatchers) {
                    Double skewnessFactor = getSkewnessFactor(this.histogram);
                    if (skewnessFactor > 2) {
                        RepartitionManager manager = new RepartitionManager(numberOfPartitions, intervalToPartitionMapping,
                                histogram.getHistogram(), getTotalWorkLoad(histogram));
                        this.intervalToPartitionMapping = manager.getRepartitionPlan();
                        this.balancedPartition = new BalancedPartition(numberOfPartitions, lowerBound, upperBound,
                                intervalToPartitionMapping);
                        collector.emit(NormalDistributionIndexingTopology.IntervalPartitionUpdateStream,
                                new Values(this.balancedPartition.getIntervalToPartitionMapping()));
                    }

                    numberOfStaticsReceived = 0;
                }
            }

        } else if (tuple.getSourceStreamId().equals(NormalDistributionIndexingTopology.FileInformationUpdateStream)) {
            String fileName = tuple.getString(0);
            Pair keyRange = (Pair) tuple.getValue(1);
            Pair timeStampRange = (Pair) tuple.getValue(2);

            filePartitionSchemaManager.add(new FileMetaData(fileName, (Double) keyRange.getKey(),
                    (Double)keyRange.getValue(), (Long) timeStampRange.getKey(), (Long) timeStampRange.getValue()));

            collector.emit(Streams.FileInformationUpdateStream,
                    new Values(fileName, keyRange, timeStampRange));
        } else if (tuple.getSourceStreamId().equals(Streams.TimeStampUpdateStream)) {
            int taskId = tuple.getSourceTask();
            Long timestamp = tuple.getLongByField("timestamp");

            indexTaskToTimestampMapping.put(taskId, timestamp);

            collector.emit(Streams.TimeStampUpdateStream, new Values(taskId, timestamp));
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declareStream(Streams.IntervalPartitionUpdateStream,
                new Fields("newIntervalPartition"));

        outputFieldsDeclarer.declareStream(Streams.FileInformationUpdateStream,
                new Fields("fileName", "keyRange", "timeStampRange"));

        outputFieldsDeclarer.declareStream(Streams.TimeStampUpdateStream,
                new Fields("taskId", "timestamp"));

        outputFieldsDeclarer.declareStream(Streams.StaticsRequestStream,
                new Fields("Statics Request"));
    }


    private double getSkewnessFactor(Histogram histogram) {

//        System.out.println("Before repartition");
//        for (int i = 0; i < intervalLoads.length; ++i) {
//            System.out.println("bin " + i + " : " + intervalLoads[i]);
//        }

        Long sum = getTotalWorkLoad(histogram);
        System.out.println("sum " + sum);
        Long maxWorkload = getMaxWorkLoad(histogram);
        System.out.println("max " + maxWorkload);
        double averageLoad = sum / (double) numberOfPartitions;

        return maxWorkload / averageLoad;
    }

    private Long getTotalWorkLoad(Histogram histogram) {
        long ret = 0;

        for(long i : histogram.histogramToList()) {
            ret += i;
        }

        return ret;
    }

    private Long getMaxWorkLoad(Histogram histogram) {
        long ret = Long.MIN_VALUE;
        for(long i : histogram.histogramToList()) {
            ret = Math.max(ret, i);
        }

        return ret;
    }


    class StatisticsRequestSendingRunnable implements Runnable {

        @Override
        public void run() {
            final int sleepTimeInSecond = 10;
            while (true) {
                try {
                    Thread.sleep(sleepTimeInSecond * 1000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }

                List<Long> counts = histogram.histogramToList();
                long sum = 0;
                for(Long count: counts) {
                    sum += count;
                }

                System.out.println(String.format("Overall Throughput: %f tuple / second", sum / (double)sleepTimeInSecond));

                histogram.clear();

                collector.emit(Streams.StaticsRequestStream,
                        new Values("Statics Request"));
            }
        }

    }
}
