package indexingTopology.util;

import backtype.storm.tuple.Tuple;
import indexingTopology.DataSchema;
import indexingTopology.exception.UnsupportedGenericException;

import java.io.IOException;
import java.util.*;


/**
 * Created by acelzj on 7/15/16.
 */
public class BulkLoader <TKey extends Comparable<TKey>, TValue> {

    private int order;

    public BTree bt;

    private TreeMap<TKey, TValue> record;

    private TimingModule tm;

    private SplitCounterModule sm;

    public BulkLoader(int btreeOrder, TimingModule tm, SplitCounterModule sm) {
        order = btreeOrder;
        this.tm = tm;
        this.sm = sm;
        record = new TreeMap<TKey, TValue>();
    }

    public void addRecord(TKey key, TValue value) {
        record.put(key, value);
    }

    public Boolean containsKey(TKey key) {
        return record.containsKey(key);
    }
    public int getNumberOfRecord() {
        return record.size();
    }
    public void resetRecord() {
        record = new TreeMap<TKey, TValue>();
    }

    public LinkedList<BTreeLeafNode> createLeaves() {
        BytesCounter counter = new BytesCounter();
        BTreeLeafNode leaf = new BTreeLeafNode(order, counter);
        LinkedList<BTreeLeafNode> leaves = new LinkedList<BTreeLeafNode>();
        BTreeLeafNode lastLeaf = leaf;
        TKey lastKey = record.firstKey();
        int count = 0;
        for (TKey key : record.keySet()) {
            if (leaf.isOverflowIntemplate()) {
                leaf.delete(lastKey);
                leaves.add(leaf);
                counter = new BytesCounter();
                leaf = new BTreeLeafNode(order, counter);
                try {
                    leaf.insertKeyValue(lastKey, record.get(lastKey));
                    leaf.insertKeyValue(key, record.get(key));
                } catch (UnsupportedGenericException e) {
                    e.printStackTrace();
                }
            } else {
                try {
                    leaf.insertKeyValue(key, record.get(key));
                    lastKey = key;
                } catch (UnsupportedGenericException e) {
                    e.printStackTrace();
                }
            }
        }
        if (leaf.isOverflowIntemplate()) {
            leaf.delete(lastKey);
            leaves.add(leaf);
            counter = new BytesCounter();
            leaf = new BTreeLeafNode(order, counter);
            try {
                leaf.insertKeyValue(lastKey, record.get(lastKey));
            } catch (UnsupportedGenericException e) {
                e.printStackTrace();
            }
            leaves.add(leaf);
        } else {
            leaves.add(leaf);
        }
        return leaves;
    }

    public BTree createTreeWithBulkLoading() {
        LinkedList<BTreeLeafNode> leaves = createLeaves();
    //    for (BTreeLeafNode leaf : leaves) {
    //        leaf.print();
    //    }
        int count = 0;
        BytesCounter counter = new BytesCounter();
        bt = new BTree(this.order, tm, sm);
        BTreeNode preNode = new BTreeLeafNode(this.order, counter);
        BTreeInnerNode root = new BTreeInnerNode(this.order, counter);
        for (BTreeLeafNode leaf : leaves) {
        //    leaf.print();
            ++count;
            if (count == 1) {
                BTreeInnerNode parent = root;
                parent.setChild(0, leaf);
                leaf.setParent(parent);
                preNode = leaf;
            } else {
                leaf.leftSibling = preNode;
                preNode.rightSibling = leaf;
                try {
                    BTreeInnerNode parent = root.getRightMostChild();
                    int index = parent.getKeyCount();
                   // System.out.println("count: = " + count + "index: = " + index + " ");
                    //  parent.print();
                    parent.setKey(index, leaf.getKey(0));
                    parent.setChild(index+1, leaf);
                    preNode = leaf;
                    //  parent.print();
                    if (parent.isOverflow()) {
                        root = (BTreeInnerNode) parent.dealOverflow(sm, leaf);
                    }
                } catch (UnsupportedGenericException e) {
                    e.printStackTrace();
                }
            }
        }
        bt.setRoot(root);
        return bt;
    }

    public double checkNewTree(BTree<TKey, TValue> indexedData, SplitCounterModule sm) {
        double numberOfRecord = 0;
        for (TKey rec : record.keySet()) {
            TValue value = record.get(rec);
            ++numberOfRecord;
            try {
                indexedData.insert(rec, value);
            } catch (UnsupportedGenericException e) {
                e.printStackTrace();
            }
        }
        System.out.println("insert failure is " + sm.getCounter() + "number of record is " + numberOfRecord);
        return ((double) sm.getCounter() / numberOfRecord);
    }
}
