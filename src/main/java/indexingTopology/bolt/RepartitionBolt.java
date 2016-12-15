package indexingTopology.bolt;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import clojure.lang.IFn;
import indexingTopology.Config.Config;
import indexingTopology.NormalDistributionIndexingTopology;
import indexingTopology.util.Histogram;
import indexingTopology.util.RepartitionManager;
import javafx.util.Pair;

import java.util.*;

/**
 * Created by acelzj on 12/12/16.
 */
public class RepartitionBolt extends BaseRichBolt {

    OutputCollector collector;

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        collector = outputCollector;
    }

    @Override
    public void execute(Tuple tuple) {
        if (tuple.getSourceStreamId().equals(NormalDistributionIndexingTopology.StatisticsReportStream)) {
            Histogram histogram = (Histogram) tuple.getValue(0);

            Map<Integer, Integer> intervalToTaskMapping = (HashMap) tuple.getValue(1);

            Double skewnessFactor = getSkewnessFactor(histogram, intervalToTaskMapping);

            int numberOfIntervals = Config.NUMBER_OF_INTERVALS;

            long[] intervalLoads = new long[numberOfIntervals];

            List<Long> frequencies = histogram.histogramToList();

            for (Integer interval : intervalToTaskMapping.keySet()) {
                intervalLoads[intervalToTaskMapping.get(interval)] += frequencies.get(interval);
            }

            if (skewnessFactor > 2) {
                RepartitionManager manager = new RepartitionManager(2, intervalToTaskMapping,
                        histogram.getHistogram(), getTotalWorkLoad(intervalLoads));
                List<Integer> taskToIntervalMapping = manager.getRepartitionPlan();
                collector.emit(NormalDistributionIndexingTopology.IntervalPartitionUpdateStream, new Values(taskToIntervalMapping));
            }
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declareStream(NormalDistributionIndexingTopology.IntervalPartitionUpdateStream,
                new Fields("newIntervalPartition"));
    }

//    }

    private double getSkewnessFactor(Histogram histogram, Map<Integer, Integer> intervalToTaskMapping) {

        final int numberOfIntervals = Config.NUMBER_OF_INTERVALS;

        long[] intervalLoads = new long[numberOfIntervals];

        List<Long> frequencies = histogram.histogramToList();

        for (Integer interval : intervalToTaskMapping.keySet()) {
            intervalLoads[intervalToTaskMapping.get(interval)] += frequencies.get(interval);
        }

        Long sum = getTotalWorkLoad(intervalLoads);
        Long maxWorkload = getMaxWorkLoad(intervalLoads);
        double averageLoad = sum / (double) numberOfIntervals;

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
}
