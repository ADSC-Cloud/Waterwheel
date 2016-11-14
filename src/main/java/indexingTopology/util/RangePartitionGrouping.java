package indexingTopology.util;

import backtype.storm.generated.GlobalStreamId;
import backtype.storm.grouping.CustomStreamGrouping;
import backtype.storm.task.WorkerTopologyContext;
import javafx.util.Pair;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by acelzj on 11/9/16.
 */
public class RangePartitionGrouping implements CustomStreamGrouping {

    Map<Integer, Pair> boltIdToKeyRange = new HashMap<Integer, Pair>();

    public void prepare(WorkerTopologyContext context, GlobalStreamId stream, List<Integer> targetTasks) {
        int numberOfBolts = targetTasks.size();
//        System.out.println("Number of bolts is " + numberOfBolts);
//        System.out.println("The bolt is " + targetTasks);
        Double minKey = 0.0;
        Double maxKey = 500.0;
        for (int i = 0; i < numberOfBolts; ++i) {
            boltIdToKeyRange.put(targetTasks.get(i), new Pair(minKey, maxKey));
            minKey = maxKey + 0.00000000000001;
            maxKey += 500.0;
        }
    }

    public List<Integer> chooseTasks(int taskId, List<Object> values) {
        List<Integer> boltIds = new ArrayList<Integer>();
        for (Integer boltId : boltIdToKeyRange.keySet()) {
            Double minKey = (Double) boltIdToKeyRange.get(boltId).getKey();
            Double maxKey = (Double) boltIdToKeyRange.get(boltId).getValue();
            if (minKey <= (Double) values.get(0) && maxKey >= (Double) values.get(0)) {
                boltIds.add(boltId);
            }
        }
        return boltIds;
    }


}
