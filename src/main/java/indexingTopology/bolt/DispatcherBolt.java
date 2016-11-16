package indexingTopology.bolt;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import indexingTopology.DataSchema;
import indexingTopology.NormalDistributionIndexingTopology;

import java.io.IOException;
import java.util.List;
import java.util.Map;

/**
 * Created by parijatmazumdar on 14/09/15.
 */
public class DispatcherBolt extends BaseRichBolt {
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


    public DispatcherBolt(DataSchema schema) {
        this.schema = schema;
    }

    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        collector = outputCollector;
//        this.nextComponentTasks=topologyContext.getComponentTasks(nextComponentID);
//        assert this.nextComponentTasks.size()==RANGE_BREAKPOINTS.length : "its hardcoded for now. lengths should match";
    }

    public void execute(Tuple tuple) {
//        double partitionValue = tuple.getDoubleByField(rangePartitionField);
        if (tuple.getSourceStreamId().equals(NormalDistributionIndexingTopology.BPlusTreeQueryStream)) {
//            collector.emit(NormalDistributionIndexingTopology.BPlusTreeQueryStream,
//                    new Values(tuple.getValue(0)));
            collector.emit(NormalDistributionIndexingTopology.BPlusTreeQueryStream,
                    new Values(tuple.getValue(0), tuple.getValue(1)));
        } else {
            try {
                collector.emit(NormalDistributionIndexingTopology.IndexStream, schema.getValuesObject(tuple));
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

/*
for (int i=0;i<RANGE_BREAKPOINTS.length;i++) {
if (partitionValue<RANGE_BREAKPOINTS[i]) {
try {
collector.emitDirect(nextComponentTasks.get(i),schema.getValuesObject(tuple));
//                    collector.emit(schema.getValuesObject(tuple));
} catch (IOException e) {
e.printStackTrace();
} finally {
break;
}
}
}
*/
//        try {
//            collector.emitDirect(nextComponentTasks.get(0),schema.getValuesObject(tuple));
//        } catch (IOException e) {
//            e.printStackTrace();
//        }
//    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) {
//        declarer.declare(schema.getFieldsObject());
//        declarer.declareStream(NormalDistributionIndexingTopology.BPlusTreeQueryStream, new Fields("key"));
        declarer.declareStream(NormalDistributionIndexingTopology.BPlusTreeQueryStream, new Fields("leftKey"
                , "rightKey"));
        declarer.declareStream(NormalDistributionIndexingTopology.IndexStream, schema.getFieldsObject());
//        declarer.declare(new Fields("key"));
    }
}
