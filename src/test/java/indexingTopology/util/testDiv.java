package indexingTopology.util;

import java.io.*;
import java.util.*;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Created by acelzj on 7/21/16.
 */
public class testDiv {
    public static void main(String[] args) throws InterruptedException {
        Map<Integer, ArrayBlockingQueue<Integer>> idToQueues = new HashMap<Integer, ArrayBlockingQueue<Integer>>();

        for (int i = 0; i < 5; ++i) {
            ArrayBlockingQueue<Integer> queue = new ArrayBlockingQueue<Integer>(10);
            for (int j = 0; j < i; ++j) {
                queue.put(j);
            }
            idToQueues.put(i, queue);
        }

        List<ArrayBlockingQueue<Integer>> taskQueues
                = new ArrayList<ArrayBlockingQueue<Integer>>(idToQueues.values());

        Collections.sort(taskQueues, new Comparator<ArrayBlockingQueue<Integer>>() {
            public int compare(ArrayBlockingQueue<Integer> taskQueue1,
                               ArrayBlockingQueue<Integer> taskQueue2) {
                return taskQueue1.size() > taskQueue2.size() ? -1 : (taskQueue1.size() < taskQueue2.size()) ? 1 : 0;
            }
        });

        for (int i = 0; i < taskQueues.size(); ++i) {
            System.out.println(taskQueues.get(i).size());
        }
    }
}
