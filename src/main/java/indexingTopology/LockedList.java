package indexingTopology;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * Created by dmir on 9/30/16.
 */
public class LockedList {
    private ArrayList<Integer> keys;

    private ArrayList<List<Integer>> values;

    private ReentrantReadWriteLock readWriteLock;

    private ReentrantReadWriteLock latch;

    public LockedList() {
        keys = new ArrayList<Integer>();
        values = new ArrayList<List<Integer>>();
        readWriteLock = new ReentrantReadWriteLock();
        latch = new ReentrantReadWriteLock();
    }

    public void insert(int key, int value) {
//        readWriteLock.readLock().tryLock();
//        readWriteLock.writeLock().tryLock();
//        accquireWriteLock();
//        try {
            System.out.println(Thread.currentThread() + "is writing + " + key);
            keys.add(key);
            values.add(new ArrayList<Integer>(Arrays.asList(value)));
            if (keys.size() > 1000) {
                split();
            }
//        } finally {
//            releaseWriteLock();
//        }
    }

    public List<Integer> get(int key) {
//        print();
//        accquireReadLock();
//        try {
            System.out.println(Thread.currentThread() + "is reading");
            int index = search(key);
            return index == -1 ? null : values.get(index);
//        } finally {
//            releaseReadLock();
//        }
    }

    private int search(int key) {
        for (int i = 0; i < keys.size(); ++i) {
            if (keys.get(i) == key) {
                return i;
            }
        }
        return -1;
    }

    public void split() {
        try {
            int midIndex = keys.size() / 2;
            ArrayList<Integer> newKeys = new ArrayList<Integer>();
            ArrayList<List<Integer>> newValues = new ArrayList<List<Integer>>();
            for (int i = 0; i < midIndex; ++i) {
                newKeys.add(keys.get(i));
                newValues.add(values.get(i));
            }
            keys = newKeys;
            values = newValues;
        } finally {

        }
    }

    public void accquireReadLock() {
        readWriteLock.readLock().lock();
    }

    public void releaseReadLock() {
        readWriteLock.readLock().unlock();
    }

    public void accquireWriteLock() {
        readWriteLock.writeLock().lock();
    }

    public void releaseWriteLock() {
        readWriteLock.writeLock().unlock();
    }

    public void print() {
        accquireReadLock();
        for (Integer key : keys) {
            System.out.print(key + " ");
        }
        System.out.println();
        releaseReadLock();
    }
}
