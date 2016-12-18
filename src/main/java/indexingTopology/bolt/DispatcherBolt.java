package indexingTopology.bolt;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import indexingTopology.Config.Config;
import indexingTopology.DataSchema;
import indexingTopology.MetaData.TaskMetaData;
import indexingTopology.MetaData.TaskPartitionSchemaManager;
import indexingTopology.NormalDistributionIndexingTopology;
import indexingTopology.util.Histogram;
import indexingTopology.util.PartitionFunction;
import indexingTopology.util.SubQuery;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.Semaphore;

/**
 * Created by acelzj on 11/17/16.
 */
public class DispatcherBolt extends BaseRichBolt{

    OutputCollector collector;
    /*
    private final String nextComponentID;
    private final DataSchema schema;
    // TODO hard coded for now. make dynamic.
    private final double [] RANGE_BREAKPOINTS = {103.8,103.85,103.90,104.00};
    private List<Integer> nextComponentTasks;
    private String rangePartitionField;
    */

    private final DataSchema schema;

    private List<Integer> targetTasks;

    private Map<Integer, Integer> intervalToTaskMapping;

    private File outputFile;

    private FileOutputStream fop;

    private Histogram histogram;

    private Double lowerBound;

    private Double upperBound;

    private Thread staticsSendingThread;

    private Semaphore staticsSendingRequest;

    private PartitionFunction partitionFunction;

    public DispatcherBolt(DataSchema schema) {
        this.schema = schema;
    }

    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        collector = outputCollector;
        outputFile = new File("/home/acelzj/IndexTopology_experiment/NormalDistribution/number_of_tasks.txt");
//        this.nextComponentTasks=topologyContext.getComponentTasks(nextComponentID);
//        assert this.nextComponentTasks.size()==RANGE_BREAKPOINTS.length : "its hardcoded for now. lengths should match";
        Set<String> componentIds = topologyContext.getThisTargets()
                .get(NormalDistributionIndexingTopology.IndexStream).keySet();
        targetTasks = new ArrayList<>();
        for (String componentId : componentIds) {
            targetTasks.addAll(topologyContext.getComponentTasks(componentId));
        }

        collector.emit(NormalDistributionIndexingTopology.IndexerNumberReportStream, new Values(targetTasks));

        lowerBound = 0.0;
        upperBound = 1000.0;

        staticsSendingRequest = new Semaphore(1);

        histogram = new Histogram();

        try {
            fop = new FileOutputStream(outputFile);
        } catch (IOException e) {
            e.printStackTrace();
        }

//        scheduleKeyRangeToTask(targetTasks);

//        InitializeTimeStamp(targetTasks);

        staticsSendingThread = new Thread(new SendStatisticsRunnable());
        staticsSendingThread.start();
    }

    public void execute(Tuple tuple) {
//        double partitionValue = tuple.getDoubleByField(rangePartitionField);
        if (tuple.getSourceStreamId().equals(NormalDistributionIndexingTopology.IndexStream)) {
            try {
                Long timeStamp = System.currentTimeMillis();
                Values values = schema.getValuesObject(tuple);

                Double indexValue = tuple.getDoubleByField(schema.getIndexField());

                updateBound(indexValue);

                while (partitionFunction == null) {
                    try {
                        Thread.sleep(1);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }

                int intervalId = partitionFunction.getIntervalId(indexValue);

                histogram.record(intervalId);

                values.add(timeStamp);


//                System.out.println("Interval Id " + intervalId);
//                System.out.println(intervalToTaskMapping == null);
//                System.out.println("key " + indexValue);
//                System.out.println("lower bound " + partitionFunction.lowerBound);
//                System.out.println("upper bound" + partitionFunction.upperBound);
                int taskId = intervalToTaskMapping.get(intervalId);

                collector.emitDirect(taskId, NormalDistributionIndexingTopology.IndexStream, values);
            } catch (IOException e) {
                e.printStackTrace();
            }
        } else {
            Map<Integer, Integer> intervalToTaskMapping = (Map) tuple.getValueByField("newIntervalPartition");
            if (intervalToTaskMapping.size() > 0) {
                this.intervalToTaskMapping = intervalToTaskMapping;
            }

            partitionFunction = (PartitionFunction) tuple.getValueByField("partitionFunction");

            staticsSendingRequest.release();
        }
    }


    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declareStream(NormalDistributionIndexingTopology.BPlusTreeQueryStream, new Fields("queryId", "key"));

        List<String> fields = schema.getFieldsObject().toList();
        fields.add("timeStamp");

//        declarer.declareStream(NormalDistributionIndexingTopology.IndexStream, schema.getFieldsObject());
        declarer.declareStream(NormalDistributionIndexingTopology.IndexStream, new Fields(fields));

        declarer.declareStream(NormalDistributionIndexingTopology.StatisticsReportStream, new Fields("statistics", "lowerBound", "upperBound"));

        declarer.declareStream(NormalDistributionIndexingTopology.IndexerNumberReportStream, new Fields("numberOfIndexers"));
    }

    private void updateBound(Double indexValue) {
        if (indexValue > upperBound) {
            upperBound = indexValue;
        }

        if (indexValue < lowerBound) {
            lowerBound = indexValue;
        }
    }


    class SendStatisticsRunnable implements Runnable {

        @Override
        public void run() {
            while (true) {
                try {
                    staticsSendingRequest.acquire();
                    Thread.sleep(10000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }

                collector.emit(NormalDistributionIndexingTopology.StatisticsReportStream,
                        new Values(histogram, lowerBound, upperBound));
            }
        }

    }
}
