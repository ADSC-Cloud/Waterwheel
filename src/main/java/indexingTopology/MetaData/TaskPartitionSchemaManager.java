package indexingTopology.MetaData;

import com.github.davidmoten.rtree.Entry;
import com.github.davidmoten.rtree.RTree;
import com.github.davidmoten.rtree.geometry.Geometries;
import com.github.davidmoten.rtree.geometry.Rectangle;
import indexingTopology.MetaData.TaskMetaData;
import rx.Observable;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by acelzj on 11/24/16.
 */
public class TaskPartitionSchemaManager {

    private RTree<TaskMetaData, Rectangle> tree = RTree.create();


    // Add a TaskMetaData to the manager
    public void add(TaskMetaData taskMetaData) {
        tree = tree.add(taskMetaData, Geometries.rectangle(taskMetaData.keyRangeLowerBound, taskMetaData.startTime,
                taskMetaData.keyRangeUpperBound, taskMetaData.endTime));
    }

    public void setStartTimeOfTask(TaskMetaData taskMetaData, Long startTime) {
        tree = tree.delete(taskMetaData, Geometries.rectangle(taskMetaData.keyRangeLowerBound, taskMetaData.startTime,
                taskMetaData.keyRangeUpperBound, taskMetaData.endTime));
        taskMetaData.setStartTime(startTime);
        tree = tree.add(taskMetaData, Geometries.rectangle(taskMetaData.keyRangeLowerBound, taskMetaData.startTime,
                taskMetaData.keyRangeUpperBound, taskMetaData.endTime));
    }

    // Retrieve the set of tasks for a given key range and time duration
    public List<Integer> search(double keyRangeLowerBound, double keyRangeUpperBound, long startTime,
                                     long endTime) {
        List<Integer> ret = new ArrayList<Integer>();
        Observable<Entry<TaskMetaData, Rectangle>> result = tree.search(Geometries.rectangle(keyRangeLowerBound,
                startTime, keyRangeUpperBound, endTime));


        for (Entry<TaskMetaData, Rectangle> e : result.toBlocking().toIterable()) {
            ret.add(e.value().taskId);
        }

        return ret;
    }

    //Retrieve the set of tasks for a given key range
    public List<Integer> keyRangedSearch(double keyRangeLowerBound, double keyRangeUpperBound) {
        return search(keyRangeLowerBound, keyRangeUpperBound, Long.MIN_VALUE, Long.MAX_VALUE);
    }

    //Retrieve the set of tasks for a given time duration
    public List<Integer> timeRangedSearch(long startTime, long endTime) {
        return search(Double.MIN_VALUE, Double.MAX_VALUE, startTime, endTime);
    }
}
