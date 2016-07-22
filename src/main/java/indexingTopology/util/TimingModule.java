package indexingTopology.util;

import indexingTopology.exception.TimingModuleException;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Created by parijatmazumdar on 18/01/16.
 */
public class TimingModule {
    private ConcurrentHashMap<String,Stack<Long>> time;
    private boolean medianComputed;
    private TimingModule() {
        time = new ConcurrentHashMap<String, Stack<Long>>();
        medianComputed = false;
    }

    public static TimingModule createNew() {
        return new TimingModule();
    }

    /* add minus start_time to time hashmap. End time is added to the
     * minus of start_time to get the time elapsed
     */
    public void startTiming(String id) {
        if (time.containsKey(id))
            time.get(id).push(-System.nanoTime());
        else {
            Stack<Long> newStack = new Stack<Long>();
            newStack.push(-System.nanoTime());
            time.put(id,newStack);
        }
    }

    public void putDuration(String id, long duration) {
        if (time.containsKey(id))
            time.get(id).push(duration);
        else {
            Stack<Long> newStack = new Stack<Long>();
            newStack.push(duration);
            time.put(id,newStack);
        }    }

    public void endTiming(String id) {
        long startTime = time.get(id).pop();
        time.get(id).push(System.nanoTime()+startTime);
    }

    public void reset() {
        time.clear();
        medianComputed = false;
    }

    public long getTotal() {
        if (!medianComputed)
            computeMedian();

        long total = 0;
        for (String k : time.keySet()) {
            total+=time.get(k).peek();
        }

        return total;
    }

    public String printTimes() {
        if (!medianComputed)
            computeMedian();

        StringBuffer sb = new StringBuffer();
        int count = time.keySet().size();
        for (String k : time.keySet()) {
            sb.append(k+":"+time.get(k).peek());
            if (count>1)
                sb.append(" , ");

            count--;
        }

        return sb.toString();
    }

    private void computeMedian() {
        medianComputed = true;
        for (String k : time.keySet()) {
            time.get(k).push(findMedian(time.get(k).toArray()));
        }
    }

    private Long findMedian(Object[] objects) {
        Arrays.sort(objects);
        if (objects.length%2==1)
            return (Long) objects[objects.length/2];
        else
            return (((Long) objects[objects.length/2]) + ((Long) objects[objects.length/2-1]))/2;
    }
}
