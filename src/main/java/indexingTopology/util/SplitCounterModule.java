package indexingTopology.util;

import java.io.*;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Stack;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Created by acelzj on 7/18/16.
 */
public class SplitCounterModule implements Serializable{
    private int counter;
    public SplitCounterModule() {
        counter = 0;
    }

    public static SplitCounterModule createNew() {
        return new SplitCounterModule();
    }

    public synchronized void addCounter() {
        ++counter;
    }

    public int getCounter() {
        return counter;
    }

    public void resetCounter() {
        counter = 0;
    }
/*    private ConcurrentHashMap<BTreeLeafNode, Integer> splits;
    private int numberOfSplits;
    private SplitCounterModule() {
        splits = new ConcurrentHashMap<BTreeLeafNode, Integer>();
        numberOfSplits = 0;
    }

    public static SplitCounterModule createNew() {
        return new SplitCounterModule();
    }

    public synchronized void updateSplitTimeOnLeaf(BTreeLeafNode leaf) {
        ++numberOfSplits;
        if (!splits.containsKey(leaf)) {
            splits.put(leaf, 1);
        } else {
            splits.put(leaf, splits.get(leaf) + 1);
        }
    }

    public int getSplitTimeOnLeaf(BTreeLeafNode leaf) {
        return splits.get(leaf);
    }

    public void reset(BTreeLeafNode leaf) {
        splits.put(leaf, 0);
    }

    public void resetSplit() {
        numberOfSplits = 0;
    }

    public int getAllSplit() {
        return numberOfSplits;
    }

    public void traverseSplit() {
        for (BTreeLeafNode leaf : splits.keySet()) {
            leaf.print();
            System.out.println(splits.get(leaf));
        }
    }*/
    public static Object deepClone(Object object) {
        try {
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            ObjectOutputStream oos = new ObjectOutputStream(baos);
            oos.writeObject(object);
            ByteArrayInputStream bais = new ByteArrayInputStream(baos.toByteArray());
            ObjectInputStream ois = new ObjectInputStream(bais);
            return ois.readObject();
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }
    }
}
