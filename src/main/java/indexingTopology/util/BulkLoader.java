package indexingTopology.util;

import backtype.storm.tuple.Tuple;
import indexingTopology.DataSchema;
import indexingTopology.exception.UnsupportedGenericException;
import javafx.util.Pair;

import java.io.IOException;
import java.util.*;


/**
 * Created by acelzj on 7/15/16.
 */
public class BulkLoader <TKey extends Comparable<TKey>, TValue> {

    private int order;

    public BTree bt;

//    private TreeMap<TKey, TValue> record;

    private List<Pair<TKey, TValue>> record;

    private TimingModule tm;

    private SplitCounterModule sm;

    public BulkLoader(int btreeOrder, TimingModule tm, SplitCounterModule sm) {
        order = btreeOrder;
        this.tm = tm;
        this.sm = sm;
//        record = new TreeMap<TKey, TValue>();
        record = new ArrayList<Pair<TKey, TValue>>();
    }

    public void addRecord(Pair pair) {
        record.add(pair);
    }

    public int getNumberOfRecord() {
        return record.size();
    }
    public void resetRecord() {
        record = new ArrayList<Pair<TKey, TValue>>();
    }

    public LinkedList<BTreeLeafNode> createLeaves() {
        BytesCounter counter = new BytesCounter();
        BTreeLeafNode leaf = new BTreeLeafNode(order, counter);
        Collections.sort(record, new Comparator<Pair>() {
            public int compare(Pair pair1, Pair pair2)
            {

                return  ((Double) pair1.getKey()).compareTo(((Double) pair2.getKey()));
            }
        });
        LinkedList<BTreeLeafNode> leaves = new LinkedList<BTreeLeafNode>();
        BTreeLeafNode lastLeaf = leaf;
        Pair lastPair = record.get(0);
        int count = 0;
        for (Pair pair : record) {
            TKey key = (TKey) pair.getKey();
            if (leaf.isOverflowIntemplate()) {
                leaf.delete((TKey) lastPair.getKey());
                leaves.add(leaf);
                counter = new BytesCounter();
                leaf = new BTreeLeafNode(order, counter);
                try {
                    leaf.insertKeyValue((TKey) lastPair.getKey(), lastPair.getValue());
                    leaf.insertKeyValue((Double) pair.getKey(), (Double) pair.getValue());
                } catch (UnsupportedGenericException e) {
                    e.printStackTrace();
                }
            } else {
                try {
                    leaf.insertKeyValue(key, pair.getValue());
                    lastPair = pair;
                } catch (UnsupportedGenericException e) {
                    e.printStackTrace();
                }
            }
        }
        if (leaf.isOverflowIntemplate()) {
            leaf.delete((TKey) lastPair.getKey());
            leaves.add(leaf);
            counter = new BytesCounter();
            leaf = new BTreeLeafNode(order, counter);
            try {
                leaf.insertKeyValue((TKey) lastPair.getKey(), lastPair.getValue());
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

  /*  public double checkNewTree(BTree<TKey, TValue> indexedData, SplitCounterModule sm) {
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
    }*/
    public boolean checkInsertion(BTree indexedData, int processedTuple) {
        int count = 0;
//        System.out.println("The size of record is " + record.size());
        for (Pair pair : record) {
            TKey key = (TKey) pair.getKey();
            if (indexedData.search(key) != null) {
                ++count;
            }
        }
       System.out.println("count = " + count);
        System.out.println("processedTuple = " + processedTuple);
        return count == processedTuple;

    }
}
