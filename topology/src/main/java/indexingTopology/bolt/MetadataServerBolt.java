package indexingTopology.bolt;

import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.google.common.hash.BloomFilter;
import indexingTopology.api.server.Server;
import indexingTopology.api.server.SystemStateQueryHandle;
import indexingTopology.bloom.DataChunkBloomFilters;
import indexingTopology.bolt.message.*;
import indexingTopology.bolt.metrics.LocationInfo;
import indexingTopology.common.Histogram;
import indexingTopology.common.KeyDomain;
import indexingTopology.common.SystemState;
import indexingTopology.common.TimeDomain;
import indexingTopology.common.data.DataSchema;
import indexingTopology.config.TopologyConfig;
import indexingTopology.filesystem.HdfsFileSystemHandler;
import indexingTopology.filesystem.LocalFileSystemHandler;
import indexingTopology.metadata.SchemaManager;
import indexingTopology.metrics.PerNodeMetrics;
import indexingTopology.util.*;
import indexingTopology.util.partition.BalancedPartition;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
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

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.*;

import static org.hsqldb.HsqlDateTime.e;

/**
 * Created by acelzj on 12/12/16.
 */
public class MetadataServerBolt<Key extends Number> extends BaseRichBolt {

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

    private List<Double> CPUloads;

    private List<Double> totalDiskSpaces;

    private List<Double> freeDiskSpaces;

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

    private SystemState systemState;

    private Server systemStateQueryServer;

    private int intervalTime;

    private int removeHours;

    private boolean removeOldDataStart;

    private SchemaManager schemaManager;

    private DataSchema defaultSchema;

    private Map<String, BloomFilter> columnToBloomFilter;

    public MetadataServerBolt(Key lowerBound, Key upperBound, DataSchema defaultSchema, TopologyConfig config) {
        this.lowerBound = lowerBound;
        this.upperBound = upperBound;
        this.config = config;
        this.defaultSchema = defaultSchema;
    }

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {

        schemaManager = new SchemaManager();
        schemaManager.setDefaultSchema(defaultSchema);

        collector = outputCollector;

        initializeMetadataFolder();

        filePartitionSchemaManager = new FilePartitionSchemaManager();

        numberOfDispatchers = topologyContext.getComponentTasks("DispatcherBolt").size();

        indexTasks = topologyContext.getComponentTasks("IndexerBolt");

        numberOfPartitions = indexTasks.size();

        balancedPartition = new BalancedPartition<>(numberOfPartitions, config.NUMBER_OF_INTERVALS, lowerBound, upperBound);

        intervalToPartitionMapping = balancedPartition.getIntervalToPartitionMapping();


        numberOfStaticsReceived = 0;

        indexTaskToTimestampMapping = new HashMap<>();

        histogram = new Histogram(config.NUMBER_OF_INTERVALS);

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


        CPUloads = Collections.synchronizedList(new ArrayList<Double>());
        totalDiskSpaces = Collections.synchronizedList(new ArrayList<>());
        freeDiskSpaces = Collections.synchronizedList(new ArrayList<>());

        systemState = new SystemState();
        staticsRequestSendingThread = new Thread(new StatisticsRequestSendingRunnable());
        staticsRequestSendingThread.start();


        String a = "aa";
        systemState.addConfig("DataChunk Dir",config.dataChunkDir);
        systemState.addConfig("Metadata Dir",config.metadataDir);
        systemState.addConfig("Load Balance Threshold", config.LOAD_BALANCE_THRESHOLD);
        systemState.addConfig("Query Servers per Node", config.CHUNK_SCANNER_PER_NODE);
        systemState.addConfig("HDFS", config.HDFSFlag);
        systemState.addConfig("Dispatchers per Node", config.DISPATCHER_PER_NODE);
        systemStateQueryServer = new Server(20000, SystemStateQueryHandle.class, new Class[]{SystemState.class}, systemState);
        systemStateQueryServer.startDaemon();
        intervalTime = config.removeIntervalHours;
        removeHours = config.previousTime;
        removeOldDataStart = false;
        columnToBloomFilter = new HashMap<>();
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
            PerNodeMetrics perNodeMetrics = (PerNodeMetrics) tuple.getValue(0);
            Histogram histogram = perNodeMetrics.histogram;
            Double cpuLoads = perNodeMetrics.CPULoad;
            Double totalDiskSpace = perNodeMetrics.totalDiskSpace;
            Double freeDiskSpace = perNodeMetrics.freeDiskSpace;
            if (numberOfStaticsReceived < numberOfDispatchers) {
                partitionLoads = new long[numberOfPartitions];
                totalDiskSpaces.add(totalDiskSpace);
                freeDiskSpaces.add(freeDiskSpace);
                this.histogram.merge(histogram);
                CPUloads.add(cpuLoads);
                ++numberOfStaticsReceived;
                if (numberOfStaticsReceived == numberOfDispatchers) {
//                    Double skewnessFactor = getSkewnessFactor(this.histogram);
//                    System.out.println("skewness factor " + skewnessFactor);
                    RangePartitioner manager = new RangePartitioner(numberOfPartitions, config.NUMBER_OF_INTERVALS, intervalToPartitionMapping,
                            this.histogram);
//                    System.out.println("Histogram: " + this.histogram);
                    Double skewnessFactor = manager.getSkewnessFactor();
                    System.out.println("skewness of key partitioning: " + skewnessFactor);
                    if (skewnessFactor > config.LOAD_BALANCE_THRESHOLD) {
                        System.out.println("skewness detected!!!");
//                        System.out.println(this.histogram.getHistogram());
//                        List<Long> workLoads = getWorkLoads(histogram);
//                        RepartitionManager manager = new RepartitionManager(numberOfPartitions, intervalToPartitionMapping,
//                                histogram.getHistogram(), getTotalWorkLoad(workLoads));
                        this.intervalToPartitionMapping = manager.getRepartitionPlan();
//                        System.out.println("after repartition " + intervalToPartitionMapping);
                        this.balancedPartition = new BalancedPartition<>(numberOfPartitions, config.NUMBER_OF_INTERVALS, lowerBound, upperBound,
                                intervalToPartitionMapping);

                        {// print skewness
                            manager = new RangePartitioner(numberOfPartitions, config.NUMBER_OF_INTERVALS, intervalToPartitionMapping, this.histogram);
                            Double newSkewnessFactor = manager.getSkewnessFactor();
                            System.out.println(String.format("Skewness: %.3f -> %.3f", skewnessFactor, newSkewnessFactor));
                        }

                        repartitionEnabled = false;
                        collector.emit(Streams.IntervalPartitionUpdateStream,
//                                new Values(this.balancedPartition.getIntervalToPartitionMapping()));
                                new Values(this.balancedPartition));

                        collector.emit(Streams.LoadBalanceStream, new Values("newIntervalPartition"));
                    } else {
//                        System.out.println("skewness is not detected!!!");
//                        System.out.println(histogram.getHistogram());
                    }

                    updateSystemState();

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
            if(removeHours == Integer.MAX_VALUE){ // topologyTest, ignore remove data
                removeOldDataStart = true;
            }
            if(removeOldDataStart == false) {
                startTimer(intervalTime, removeHours); // Remove old data regularly
                removeOldDataStart = true;
            }

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
                metaDataOutput.close();
            }


//            System.out.println("File information is received on metedata servers");

            DataChunkBloomFilters bloomFilters = (DataChunkBloomFilters) tuple.getValueByField("bloomFilters");

            columnToBloomFilter = bloomFilters.columnToBloomFilter;
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
        } else if (tuple.getSourceStreamId().equals(Streams.DDLRequestStream)) {
            AsyncRequestMessage request = (AsyncRequestMessage)tuple.getValue(0);
            AsyncResponseMessage response = null;
            if (request instanceof AsyncSchemaCreateRequest) {
                final String name = ((AsyncSchemaCreateRequest) request).name;
                final DataSchema schema = ((AsyncSchemaCreateRequest) request).schema;
                boolean isSuccessful = schemaManager.createSchema(name, schema);
                response = new AsyncSchemaCreateResponse(name, isSuccessful, request.id);
            } else if (request instanceof AsyncSchemaQueryRequest) {
                final String name = ((AsyncSchemaQueryRequest) request).name;
                DataSchema schema = schemaManager.getSchema(name);
                response = new AsyncSchemaQueryResponse(name, schema, request.id);
            }
            collector.emit(Streams.DDLResponseStream, new Values(response));
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

        outputFieldsDeclarer.declareStream(Streams.OldDataRemoval,
                new Fields("fileName", "columnToBloomFilter"));

        outputFieldsDeclarer.declareStream(Streams.TimestampUpdateStream,
                new Fields("taskId", "keyDomain", "timeDomain"));

        outputFieldsDeclarer.declareStream(Streams.StaticsRequestStream,
                new Fields("Statics Request"));


        outputFieldsDeclarer.declareStream(Streams.LoadBalanceStream, new Fields("loadBalance"));

        outputFieldsDeclarer.declareStream(Streams.LocationInfoUpdateStream, new Fields("info"));

        outputFieldsDeclarer.declareStream(Streams.DDLResponseStream, new Fields("response"));
    }

    @Override
    public void cleanup() {
        super.cleanup();
        staticsRequestSendingThread.interrupt();
        systemStateQueryServer.endDaemon();
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

    private void updateSystemState() {
        double totalCPULoad = .0;
        for (int i = 0; i < CPUloads.size(); i++) {
            totalCPULoad += CPUloads.get(i);
        }
        systemState.setCpuRatio(totalCPULoad / CPUloads.size());
        CPUloads.clear();

        double totalDiskSpace = .0;
        for (double i: totalDiskSpaces) {
            totalDiskSpace += i;
        }
        systemState.setTotalDiskSpaceInGB(totalDiskSpace / config.DISPATCHER_PER_NODE);
        totalDiskSpaces.clear();

        double freeDiskSpace = .0;
        for (double i: freeDiskSpaces) {
            freeDiskSpace += i;
        }
        systemState.setAvailableDiskSpaceInGB(freeDiskSpace / config.DISPATCHER_PER_NODE);
        freeDiskSpaces.clear();


        List<Long> counts = histogram.histogramToList();
        long sum = 0;
        for(Long count: counts) {
            sum += count;
        }
        systemState.setThroughout(sum / (double)TopologyConfig.StaticRequestTimeIntervalInSeconds);

        // evict the oldest value to make room for the latest one.
        for (int i = 0; i < SystemState.NumberOfHistoricThroughputs - 1; i++) {
            systemState.lastThroughput[i] = systemState.lastThroughput[i + 1];
        }
        systemState.lastThroughput[SystemState.NumberOfHistoricThroughputs - 1] = systemState.getThroughput();
        System.out.println(String.format("Overall Throughput: %f tuple / second.", systemState.getThroughput()));
        System.out.println(String.format("CPU utilization: %f.", systemState.getRatio()));
        System.out.println(String.format("total disk: %.2f GN.", systemState.getTotalDiskSpaceInGB()));
        System.out.println(String.format("free disk: %.2f GB.", systemState.getAvailableDiskSpaceInGB()));
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

    private void initializeMetadataFolder() {
        Runtime runtime = Runtime.getRuntime();
        try {
            runtime.exec("mkdir -p " + config.metadataDir);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }


    public static String getCurrentTime() {
        Date date = new Date();
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        return sdf.format(date);
    }

//    public static void main(String[] args) throws InterruptedException {
//        System.out.println("main start:" + getCurrentTime());
//        startTimer();
//    }

    public void startTimer(int intervalTime,int removeHours) {
        TimerTask task = new TimerTask() {
            @Override
            public void run() {
                Calendar time = Calendar.getInstance();
                int currentHour = 0;
                time.get(currentHour);
                if (config.HDFSFlag == false) {
                    // Local FileSystem
                    LocalFileSystemHandler localFileSystemHandler = new LocalFileSystemHandler("../", config);
                    File folderOfData = new File(config.dataChunkDir);
                    File folderOfMetaData = new File(config.metadataDir);
                    searchLocalOldData(folderOfData,localFileSystemHandler,removeHours,false);
//                    searchLocalOldData(folderOfMetaData,localFileSystemHandler,removeHours,true);
                }
                else{
                    // HDFS
                    try {
                        HdfsFileSystemHandler fileSystemHandler = new HdfsFileSystemHandler("", config);
                        fileSystemHandler.openFile(config.dataChunkDir,"");
                        try {
                            searchHDFSOldData(fileSystemHandler, config.dataChunkDir, removeHours, false);
                        } catch (InterruptedException e1) {
                            e1.printStackTrace();
                        }
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                }
            }
        };
        Timer timer = new Timer();
        timer.schedule(task, buildTime(), 1000 * 30);
//        timer.schedule(task, buildTime(), 3600 * 1000 * intervalTime);
    }

    public void searchHDFSOldData(HdfsFileSystemHandler fileSystemHandler,String relativePath,int removeHours, boolean isMetadata) throws InterruptedException, IOException {
        FileStatus[] fileStatus = fileSystemHandler.getFileSystem().listStatus(new Path(relativePath));
        List<FileMetaData> removalFileMeta = filePartitionSchemaManager.searchFileMetaData(Double.MIN_VALUE, Double.MAX_VALUE, 0, System.currentTimeMillis() - 3600 * 1000 * removeHours);
        if(removalFileMeta.size() != 0) {
            for (int i = 0; i < removalFileMeta.size(); i++) {
                if (removalFileMeta.get(i).getEndTime() > System.currentTimeMillis() - 3600 * 1000 * removeHours) {
                    removalFileMeta.remove(i);
                }
            }
            for (int i = 0; i < removalFileMeta.size(); i++) {
                try {
                    fileSystemHandler.removeOldData(new Path(relativePath + "/" + removalFileMeta.get(i).getFilename()));
                    filePartitionSchemaManager.remove(removalFileMeta.get(i));
                    collector.emit(Streams.OldDataRemoval, new Values(removalFileMeta.get(i).getFilename(), columnToBloomFilter));
                } catch (InterruptedException e1) {
                    e1.printStackTrace();
                }
            }
        }
//        for(FileStatus singleFile : fileStatus) {
//            if (System.currentTimeMillis() - singleFile.getModificationTime() >= 3600 * removeHours) {
////                System.out.println("---------------" + singleFile.getPath().getName() + "---------------");
//                fileSystemHandler.removeOldData(singleFile.getPath());
//            }
//        }
    }

    public void searchLocalOldData(File folder,LocalFileSystemHandler localFileSystemHandler,int removeHours, boolean isMetadata){
//        System.out.println(folder);

//        List<FileMetaData> removalFileMeta = filePartitionSchemaManager.searchFileMetaData(Double.MIN_VALUE, Double.MAX_VALUE, 0, System.currentTimeMillis() - 3600 * 1000 * removeHours);
        List<FileMetaData> removalFileMeta = filePartitionSchemaManager.searchFileMetaData(Double.MIN_VALUE, Double.MAX_VALUE, 0, System.currentTimeMillis() - 30 * 1000);
        System.out.println("removalFileMeta:" + removalFileMeta.size());
        if(removalFileMeta.size() != 0){
            System.out.println(System.currentTimeMillis() - 3600 * 1000 * removeHours + "    " + removalFileMeta.get(0).getStartTime());
            for(int i = 0; i < removalFileMeta.size(); i++){
            if(removalFileMeta.get(i).getEndTime() > System.currentTimeMillis() - 1000 * 30){
//                if(removalFileMeta.get(i).getEndTime() > System.currentTimeMillis() - 3600 * 1000 * removeHours){
                    removalFileMeta.remove(i);
                }
            }
            for(int i = 0; i < removalFileMeta.size(); i++){
//            System.out.println("removalFile : " + removalFileMeta.get(i).getFilename());
                try {
                    localFileSystemHandler.removeOldData(folder.getPath() + "/" + removalFileMeta.get(i).getFilename());
                    filePartitionSchemaManager.remove(removalFileMeta.get(i));
                    collector.emit(Streams.OldDataRemoval, new Values(removalFileMeta.get(i).getFilename(), columnToBloomFilter));
                } catch (InterruptedException e1) {
                    e1.printStackTrace();
                }
            }
        }
    }

    private Date buildTime() {
        Calendar calendar = Calendar.getInstance();
        calendar.set(Calendar.HOUR_OF_DAY, 8);
        calendar.set(Calendar.MINUTE, 0);
        calendar.set(Calendar.SECOND, 0);
        Date time = calendar.getTime();
//        if (time.before(new Date())) {
//            time = addDay(time, 1);
//        }
        return time;
    }

    private Date addDay(Date date, int days) {
        Calendar startDT = Calendar.getInstance();
        startDT.setTime(date);
        startDT.add(Calendar.DAY_OF_MONTH, days);
        return startDT.getTime();
    }

    public FilePartitionSchemaManager getFilePartitionSchemaManager(){
        return filePartitionSchemaManager;
    }

    class StatisticsRequestSendingRunnable implements Runnable {

        @Override
        public void run() {

//            while (true) {
            systemState.setLastThroughput(new double[SystemState.NumberOfHistoricThroughputs]);

            while (!Thread.currentThread().isInterrupted()) {
                try {
                    Thread.sleep(TopologyConfig.StaticRequestTimeIntervalInSeconds * 1000);
                } catch (InterruptedException e) {
//                    e.printStackTrace();
                    Thread.currentThread().interrupt();
                }

//                List<Long> counts = histogram.histogramToList();
//                long sum = 0;
//                for(Long count: counts) {
//                    sum += count;
//                }
//                systemState.setThroughout(sum / (double)sleepTimeInSecond);
//                systemState.setCpuRatio(60);
//                systemState.setDiskRatio(40);
//                int throughputSite = systemState.lastThroughput.length-1;
//                while(throughputSite > 0){
//                    systemState.lastThroughput[throughputSite] = systemState.lastThroughput[throughputSite-1];
//                    throughputSite--;
//                }
//                   System.out.println("statics request has been sent!!!");

                histogram.clear();

                collector.emit(Streams.StaticsRequestStream,
                        new Values("Statics Request"));

            }
        }

    }
}