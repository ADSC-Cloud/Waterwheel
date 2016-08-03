package indexingTopology.util;

import indexingTopology.exception.TimingModuleException;

import java.io.*;
import java.math.BigInteger;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Created by parijatmazumdar on 18/01/16.
 */
public class TimingModule implements Serializable{
  //  private ConcurrentHashMap<String,Stack<Long>> time;
  private ConcurrentHashMap<String,Stack<Long>> time;
    private boolean medianComputed;
    private int numEndTime = 0;
    private int numStartTime = 0;
    private TimingModule() {
        time = new ConcurrentHashMap<String, Stack<Long>>();
        medianComputed = false;
        numEndTime = 0;
        numStartTime = 0;
    }

    public static TimingModule createNew() {
        return new TimingModule();
    }

    /* add minus start_time to time hashmap. End time is added to the
     * minus of start_time to get the time elapsed
     */
    public void startTiming(String id) {
        ++numStartTime;
        if (time.containsKey(id)) {
            time.get(id).push(-System.nanoTime());

            //      time.get(id).push(-System.currentTimeMillis());
        } else {
         //   Stack<Long> newStack = new Stack<Long>();
            Stack<Long> newStack = new Stack<Long>();
            long currentTime = -System.nanoTime();
            newStack.push(-System.nanoTime());

        //    newStack.push(-System.currentTimeMillis());
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
        }
    }

    public void putChunkStartTime(String id) {
        if (!time.containsKey(id)) {
            Stack<Long> newStack = new Stack<Long>();
            newStack.push(-System.nanoTime());
            time.put(id, newStack);
            System.out.println("new stack has been put into time");
        }
    }

    public long getChunkStartTime() {
        return time.get(Constants.TIME_CHUNK_START).peek();
    }

    public void endTiming(String id) {
        long startTime = time.get(id).pop();
        time.get(id).push(System.nanoTime()+startTime);
        ++numEndTime;
    //    time.get(id).push(System.currentTimeMillis()+startTime);
    }

    public int getNumEndTime() {
        return numEndTime;
    }

    public int getNumStartTime() {
        return numStartTime;
    }

    public void reset() {
        time.clear();
        numEndTime = 0;
        numStartTime = 0;
        medianComputed = false;
    }

    public long getTotal() {
    //    if (!medianComputed)
    //        computeMedian();

        long total = 0;
    /*    for (String k : time.keySet()) {
            Stack<Long> stack = time.get(k);
            while (!stack.empty()) {
                total += stack.pop();
            }
        //    total+=time.get(k).peek();
        }*/

        total += getSplitTime();
        total += getInsertionTime();
        total += getFindTime();

        return total;
    }

    public long getSplitTime() {

        long total = 0;
        if (time.containsKey(Constants.TIME_SPLIT.str)) {
            Stack<Long> stack = time.get(Constants.TIME_SPLIT.str);
            while (!stack.empty()) {
                total += stack.pop();
            }
        }
        return total;

    }

    public long getFindTime() {

        long total = 0;
        if (time.containsKey(Constants.TIME_LEAF_FIND.str)) {
            Stack<Long> stack = time.get(Constants.TIME_LEAF_FIND.str);
            while (!stack.empty()) {
                total += stack.pop();
            }
        }
        return total;

    }

    public long getInsertionTime() {

        long total = 0;
        if (time.containsKey(Constants.TIME_LEAF_INSERTION.str)) {
            Stack<Long> stack = time.get(Constants.TIME_LEAF_INSERTION.str);
            while (!stack.empty()) {
                total += stack.pop();
            }
        }
        return total;

    }

    public long getSerializeTime() {

        long total = 0;
        if (time.containsKey(Constants.TIME_SERIALIZATION_WRITE.str)) {
            Stack<Long> stack = time.get(Constants.TIME_SERIALIZATION_WRITE.str);
            while (!stack.empty()) {
                total += stack.pop();
            }
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

    public static Object deepClone(Object object) {
        try {
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            ObjectOutputStream oos = new ObjectOutputStream(baos);
            oos.writeObject(object);
            ByteArrayInputStream bais = new ByteArrayInputStream(baos.toByteArray());
            ObjectInputStream ois = new ObjectInputStream(bais);
            return ois.readObject();
        }
        catch (Exception e) {
            e.printStackTrace();
            return null;
        }
    }
}
