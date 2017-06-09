package indexingTopology.bolt;

import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import indexingTopology.bloom.DataChunkBloomFilters;
import indexingTopology.bolt.metrics.LocationInfo;
import indexingTopology.config.TopologyConfig;
import indexingTopology.util.*;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import indexingTopology.metadata.FileMetaData;
import indexingTopology.metadata.FilePartitionSchemaManager;
import indexingTopology.streams.Streams;
import org.apache.zookeeper.KeeperException;

import java.util.*;

/**
 * Created by acelzj on 12/12/16.
 */
public class MetadataServer <Key extends Number> extends BaseRichBolt {

    private OutputCollector collector;

    private Map<Integer, Integer> intervalToPartitionMapping;

    private Key upperBound;

    private Key lowerBound;

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

    private boolean repartitionEnabled;

    private ZookeeperHandler zookeeperHandler;

    private Output output;

    private int numberOfFiles;

    private String path = "/MetadataNode";

    private Long maxTimestamp;

    private Long minTimestamp;

    private int targetFileNums = 30;

    private Long numTuples;

    private Map<Integer, Integer> taskIdToFileNumMapping;

    private TopologyConfig config;

    public MetadataServer(Key lowerBound, Key upperBound, TopologyConfig config) {
        this.lowerBound = lowerBound;
        this.upperBound = upperBound;
        this.config = config;
    }

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        collector = outputCollector;

        filePartitionSchemaManager = new FilePartitionSchemaManager();

        numberOfDispatchers = topologyContext.getComponentTasks("DispatcherBolt").size();

        indexTasks = topologyContext.getComponentTasks("IndexerBolt");

        numberOfPartitions = indexTasks.size();

        balancedPartition = new BalancedPartition<>(numberOfPartitions, lowerBound, upperBound, config);

        intervalToPartitionMapping = balancedPartition.getIntervalToPartitionMapping();


        numberOfStaticsReceived = 0;

        indexTaskToTimestampMapping = new HashMap<>();

        histogram = new Histogram(config);

        repartitionEnabled = true;

        numberOfFiles = 0;

        numTuples = 0L;

        output = new Output(50000, 6500000);

        minTimestamp = Long.MAX_VALUE;
        maxTimestamp = Long.MIN_VALUE;

        taskIdToFileNumMapping = new HashMap<>();

//        try {
//            zookeeperHandler = new ZookeeperHandler();
//        } catch (Exception e) {
//            e.printStackTrace();
//        }

        staticsRequestSendingThread = new Thread(new StatisticsRequestSendingRunnable());
        staticsRequestSendingThread.start();


//        createMetadataSendingThread();
    }

    private void createMetadataSendingThread() {
        new Thread(new Runnable() {
            @Override
            public void run() {
                try {
                    byte[] bytes;
                    bytes = zookeeperHandler.getData(path);

                    Input input = new Input(bytes);

                    int numberOfMetadata = input.readInt();
                    for (int i = 0; i < numberOfMetadata; ++i) {
                        String fileName = input.readString();
                        Double lowerBound = input.readDouble();
                        Double upperBound = input.readDouble();
                        Long startTimestamp = input.readLong();
                        Long endTimestamp = input.readLong();
                        collector.emit(Streams.FileInformationUpdateStream,
                                new Values(fileName, new KeyDomain(lowerBound, upperBound),
                                        new TimeDomain(startTimestamp, endTimestamp)));
                    }
                } catch (InterruptedException e) {
                    e.printStackTrace();
                } catch (KeeperException e) {
                    e.printStackTrace();
                }
            }

        }).start();
    }

    @Override
    public void execute(Tuple tuple) {

        if (tuple.getSourceStreamId().equals(Streams.StatisticsReportStream)) {

            Histogram histogram = (Histogram) tuple.getValue(0);

            if (numberOfStaticsReceived < numberOfDispatchers) {
                partitionLoads = new long[numberOfPartitions];
                this.histogram.merge(histogram);
                ++numberOfStaticsReceived;
                if (numberOfStaticsReceived == numberOfDispatchers) {
//                    Double skewnessFactor = getSkewnessFactor(this.histogram);
//                    System.out.println("skewness factor " + skewnessFactor);
                    RepartitionManager manager = new RepartitionManager(numberOfPartitions, intervalToPartitionMapping,
                            this.histogram, config);
                    Double skewnessFactor = manager.getSkewnessFactor();
                    if (skewnessFactor > config.LOAD_BALANCE_THRESHOLD) {
//                        System.out.println("skewness detected!!!");
//                        System.out.println(this.histogram.getHistogram());
//                        List<Long> workLoads = getWorkLoads(histogram);
//                        RepartitionManager manager = new RepartitionManager(numberOfPartitions, intervalToPartitionMapping,
//                                histogram.getHistogram(), getTotalWorkLoad(workLoads));
                        this.intervalToPartitionMapping = manager.getRepartitionPlan();
//                        System.out.println("after repartition " + intervalToPartitionMapping);
                        this.balancedPartition = new BalancedPartition<>(numberOfPartitions, lowerBound, upperBound,
                                intervalToPartitionMapping, config);
                        repartitionEnabled = false;
                        collector.emit(Streams.IntervalPartitionUpdateStream,
//                                new Values(this.balancedPartition.getIntervalToPartitionMapping()));
                                new Values(this.balancedPartition));

                        collector.emit(Streams.LoadBalanceStream, new Values("newIntervalPartition"));
                    } else {
//                        System.out.println("skewness is not detected!!!");
//                        System.out.println(histogram.getHistogram());
                    }

                    numberOfStaticsReceived = 0;
                }
            }

        } else if (tuple.getSourceStreamId().equals(Streams.FileInformationUpdateStream)) {
            String fileName = tuple.getString(0);
            TimeDomain timeDomain = (TimeDomain) tuple.getValueByField("timeDomain");
            KeyDomain keyDomain = (KeyDomain) tuple.getValueByField("keyDomain");
            Long tupleCount = tuple.getLongByField("tupleCount");
            filePartitionSchemaManager.add(new FileMetaData(fileName, (Double) keyDomain.getLowerBound(),
                    (Double)keyDomain.getUpperBound(), timeDomain.getStartTimestamp(), timeDomain.getEndTimestamp()));

//            System.out.println(timeDomain.getEndTimestamp() - timeDomain.getStartTimestamp());

            int taskId = tuple.getSourceTask();



            if (taskIdToFileNumMapping.get(taskId) == null) {
                taskIdToFileNumMapping.put(taskId, 1);
            } else {
                taskIdToFileNumMapping.put(taskId, taskIdToFileNumMapping.get(taskId) + 1);
            }


            ++numberOfFiles;

            Long timestampUpperBound = timeDomain.getEndTimestamp();
            Long timestampLowerBound = timeDomain.getStartTimestamp();

            if (numberOfFiles <= targetFileNums) {
                if (timestampLowerBound < minTimestamp) {
                    minTimestamp = timestampLowerBound;
                }

                if (timestampUpperBound > maxTimestamp) {
                    maxTimestamp = timestampUpperBound;
                }

                numTuples += tupleCount;
            }

            WriteMetaData(fileName, keyDomain, timeDomain);

            if (numberOfFiles == targetFileNums) {
                Output metaDataOutput = new Output(50000, 65000000);
                metaDataOutput.writeInt(numberOfFiles);
                metaDataOutput.write(output.toBytes());
                try {
                    if (zookeeperHandler != null) {
                        zookeeperHandler.create(path, metaDataOutput.toBytes());
                        System.out.println("Metadata has been written to zookeeper!!!");
                        System.out.println("timestamp lower bound " + minTimestamp);
                        System.out.println("timestamp upper bound " + maxTimestamp);
                    }
//                    System.out.println(taskIdToFileNumMapping);
//                    System.out.println("tuple count" + numTuples);
                } catch (KeeperException e) {
                    e.printStackTrace();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }


//            System.out.println("File information is received on metedata servers");

            DataChunkBloomFilters bloomFilters = (DataChunkBloomFilters) tuple.getValueByField("bloomFilters");

            // omit the logic of storing bloomFilter externally, simply forwarding to the query coordinator.

//            System.out.println("File information is sent from metedata servers");
            collector.emit(Streams.FileInformationUpdateStream,
                    new Values(fileName, keyDomain, timeDomain, bloomFilters));
        } else if (tuple.getSourceStreamId().equals(Streams.TimestampUpdateStream)) {
            int taskId = tuple.getSourceTask();
            TimeDomain timeDomain = (TimeDomain) tuple.getValueByField("timeDomain");
            KeyDomain keyDomain = (KeyDomain) tuple.getValueByField("keyDomain");
            Long endTimestamp = timeDomain.getEndTimestamp();

            indexTaskToTimestampMapping.put(taskId, endTimestamp);

            collector.emit(Streams.TimestampUpdateStream, new Values(taskId, keyDomain, timeDomain));
        } else if (tuple.getSourceStreamId().equals(Streams.EnableRepartitionStream)) {
            repartitionEnabled = true;
//            System.out.println("repartition has been enabled!!!");
        } else if (tuple.getSourceStreamId().equals(Streams.LocationInfoUpdateStream)) {
            LocationInfo info = (LocationInfo) tuple.getValue(0);
            //TODO: maintain the info in mete-data server as well as the backing store.

            // simply forward the info
            collector.emit(Streams.LocationInfoUpdateStream, new Values(info));
        }
    }

    private void WriteMetaData(String fileName, KeyDomain keyDomain, TimeDomain timeDomain) {
        output.writeString(fileName);
        output.writeDouble((Double) keyDomain.getLowerBound());
        output.writeDouble((Double) keyDomain.getUpperBound());
        output.writeLong(timeDomain.getStartTimestamp());
        output.writeLong(timeDomain.getEndTimestamp());
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declareStream(Streams.IntervalPartitionUpdateStream,
                new Fields("newIntervalPartition"));

        outputFieldsDeclarer.declareStream(Streams.FileInformationUpdateStream,
                new Fields("fileName", "keyDomain", "timeDomain", "bloomFilters"));

        outputFieldsDeclarer.declareStream(Streams.TimestampUpdateStream,
                new Fields("taskId", "keyDomain", "timeDomain"));

        outputFieldsDeclarer.declareStream(Streams.StaticsRequestStream,
                new Fields("Statics Request"));


        outputFieldsDeclarer.declareStream(Streams.LoadBalanceStream, new Fields("loadBalance"));

        outputFieldsDeclarer.declareStream(Streams.LocationInfoUpdateStream, new Fields("info"));
    }

    @Override
    public void cleanup() {
        super.cleanup();
        staticsRequestSendingThread.interrupt();
    }


    private double getSkewnessFactor(Histogram histogram) {

        List<Long> workLoads = getWorkLoads(histogram);

        Long sum = getTotalWorkLoad(workLoads);
//        Long sum = getTotalWorkLoad(histogram);
//        Long maxWorkload = getMaxWorkLoad(histogram);
        Long maxWorkload = getMaxWorkLoad(workLoads);
        double averageLoad = sum / (double) numberOfPartitions;

        return maxWorkload / averageLoad;
    }


    public List<Long> getWorkLoads(Histogram histogram) {
        Map<Integer, Integer> intervalToPartitionMapping = balancedPartition.getIntervalToPartitionMapping();

        List<Long> wordLoads = new ArrayList<>();

        int partitionId = 0;

        long tmpWorkload = 0;

        List<Long> workLoads = histogram.histogramToList();

        for (int intervalId = 0; intervalId < config.NUMBER_OF_INTERVALS; ++intervalId) {
            if (intervalToPartitionMapping.get(intervalId) != partitionId) {
                wordLoads.add(tmpWorkload);
                tmpWorkload = 0;
                partitionId = intervalToPartitionMapping.get(intervalId);
            }

            tmpWorkload += workLoads.get(intervalId);
        }

        wordLoads.add(tmpWorkload);

        return wordLoads;
    }

//    public Long getTotalWorkLoad(Histogram histogram) {
//        long ret = 0;
//
//        for(long i : histogram.histogramToList()) {
//            ret += i;
//        }
//
//        return ret;
//    }

    public Long getTotalWorkLoad(List<Long> workLoads) {
        long ret = 0;

        for(long i : workLoads) {
            ret += i;
        }

        return ret;
    }

//    private Long getMaxWorkLoad(Histogram histogram) {
//        long ret = Long.MIN_VALUE;
//        for(long i : histogram.histogramToList()) {
//            ret = Math.max(ret, i);
//        }
//        Map<Integer, Integer> intervalToPartitionMapping = balancedPartition.getIntervalToPartitionMapping();
//
//        int partitionId = 0;

//        long tmpWorkload = 0;
//
//        List<Long> workLoads = histogram.histogramToList();
//
//        for (int intervalId = 0; intervalId < TopologyConfig.NUMBER_OF_INTERVALS; ++intervalId) {
//            if (intervalToPartitionMapping.get(intervalId) != partitionId) {
//                ret = Math.max(ret, tmpWorkload);
//                tmpWorkload = 0;
//                partitionId = intervalToPartitionMapping.get(intervalId);
//            }
//
//            tmpWorkload += workLoads.get(intervalId);
//        }
//
//        ret = Math.max(ret, tmpWorkload);
//        return ret;
//    }
    private Long getMaxWorkLoad(List<Long> workLoads) {
        long ret = 0;

        for(long i : workLoads) {
            ret = Math.max(ret, i);
        }

        return ret;
    }


    class StatisticsRequestSendingRunnable implements Runnable {

        @Override
        public void run() {
            final int sleepTimeInSecond = 10;
//            while (true) {
            while (!Thread.currentThread().isInterrupted()) {
                try {
                    Thread.sleep(sleepTimeInSecond * 1000);
                } catch (InterruptedException e) {
//                    e.printStackTrace();
                    Thread.currentThread().interrupt();
                }

                List<Long> counts = histogram.histogramToList();
                long sum = 0;
                for(Long count: counts) {
                    sum += count;
                }

//                System.out.println("statics request has been sent!!!");
//                System.out.println(String.format("Overall Throughput: %f tuple / second", sum / (double)sleepTimeInSecond));

                histogram.clear();

                if (repartitionEnabled) {
                    collector.emit(Streams.StaticsRequestStream,
                            new Values("Statics Request"));
                }
            }
        }

    }
}
