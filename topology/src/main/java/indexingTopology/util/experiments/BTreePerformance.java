package indexingTopology.util.experiments;

import indexingTopology.config.TopologyConfig;
import indexingTopology.exception.UnsupportedGenericException;
import indexingTopology.metrics.TimeMetrics;
import indexingTopology.index.BTree;
import javafx.util.Pair;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.Random;

/**
 * Created by robert on 13/7/17.
 */
public class BTreePerformance {
    static public void main(String[] args) {
        final boolean bulk_loading = true;
        TopologyConfig config = new TopologyConfig();
        final int numberOfTuples = 1024 * 1024;
        ArrayList<Pair<Integer, byte[]>> tuples = new ArrayList<>();
        Random random = new Random();
        byte[] bytes = new byte[10];
        for (int i = 0; i < numberOfTuples; i++) {
            tuples.add(new Pair<>(random.nextInt(), bytes));
        }

        BTree<Integer, Long> bTree = new BTree<>(64, config);

        TimeMetrics timeMetrics = new TimeMetrics();
        timeMetrics.startEvent("insert");

        if (bulk_loading) {
            timeMetrics.startEvent("sort");
            tuples.sort(Comparator.comparing(Pair<Integer, byte[]>::getKey));
//					(t1, t2) -> {
//				return Integer.compare(((Pair<Integer, byte[]>)t1).getKey(), ((Pair<Integer, byte[]>)t2).getKey());
//			});
            timeMetrics.endEvent("sort");
        }

        tuples.forEach(t -> {
            try {
                bTree.insert(t.getKey(), t.getValue());
            } catch (UnsupportedGenericException e) {
                e.printStackTrace();
            }
        });
        timeMetrics.endEvent("insert");

        BTree<Integer, Long> templateBTree = bTree.getTemplate();
        templateBTree.clearPayload();
        timeMetrics.startEvent("template insert 1");
        tuples.forEach(t -> {
            try {
                templateBTree.insert(t.getKey(), t.getValue());
            } catch (UnsupportedGenericException e) {
                e.printStackTrace();
            }
        });
        timeMetrics.endEvent("template insert 1");

        templateBTree.clearPayload();
        timeMetrics.startEvent("template insert 2");
        tuples.forEach(t -> {
            try {
                templateBTree.insert(t.getKey(), t.getValue());
            } catch (UnsupportedGenericException e) {
                e.printStackTrace();
            }
        });
        timeMetrics.endEvent("template insert 2");

        templateBTree.clearPayload();
        timeMetrics.startEvent("template insert 3");
        tuples.forEach(t -> {
            try {
                templateBTree.insert(t.getKey(), t.getValue());
            } catch (UnsupportedGenericException e) {
                e.printStackTrace();
            }
        });
        timeMetrics.endEvent("template insert 3");

        System.out.println(timeMetrics);

    }

}
