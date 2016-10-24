package indexingTopology.util;

import backtype.storm.tuple.Tuple;
import indexingTopology.DataSchema;
import indexingTopology.exception.UnsupportedGenericException;
import javafx.util.Pair;
import junit.framework.TestCase;

import java.io.IOException;
import java.math.BigInteger;
import java.util.*;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * Created by acelzj on 7/18/16.
 */
public class testCreateLeaves_dataPages <TKey extends Comparable<TKey>, TValue>{

    private int order;

    private String indexField;

    private ArrayList<Tuple> tuples;

    private int bytesLimit;

    public BTree bt;

    private List<Pair<TKey, TValue>> record;

    private TimingModule tm;

    private BytesCounter counter;

    private boolean templateMode;

    private SplitCounterModule sm;

    public testCreateLeaves_dataPages() {
        this.order = 4;
        counter = new BytesCounter();
        SplitCounterModule sm = new SplitCounterModule();
        templateMode = false;
    }

    public List createRecord(List<TKey> keys) {
        record = new ArrayList<Pair<TKey, TValue>>();
        int offset = 0;
        for (TKey key : keys) {
            Pair pair = new Pair(key, offset);
            record.add(pair);
        }
        return record;
    }
    public LinkedList<BTreeLeafNode> createLeaves(List<TKey> keys) {
        createRecord(keys);
        counter.increaseHeightCount();
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
        tm = TimingModule.createNew();
        int count = 0;
        for (Pair pair : record) {
            TKey key = (TKey) pair.getKey();
            if (leaf.isOverflow()) {
                leaf.delete((TKey) lastPair.getKey());
                leaves.add(leaf);
                leaf = new BTreeLeafNode(order, counter);
                try {
                    leaf.insertKeyValueInBulkLoading((TKey) lastPair.getKey(), lastPair.getValue());
                    leaf.insertKeyValueInBulkLoading((Double) pair.getKey(),  pair.getValue());
                } catch (UnsupportedGenericException e) {
                    e.printStackTrace();
                }
            } else {
                try {
                    leaf.insertKeyValueInBulkLoading(key, pair.getValue());
                    lastPair = pair;
                } catch (UnsupportedGenericException e) {
                    e.printStackTrace();
                }
            }
        }
        if (leaf.isOverflow()) {
            leaf.delete((TKey) lastPair.getKey());
            leaves.add(leaf);
            leaf = new BTreeLeafNode(order, counter);
            try {
                leaf.insertKeyValueInBulkLoading((TKey) lastPair.getKey(), lastPair.getValue());
            } catch (UnsupportedGenericException e) {
                e.printStackTrace();
            }
            leaves.add(leaf);
        } else {
            leaves.add(leaf);
        }
        return leaves;
    }



    public BTree createTreeWithBulkLoading(LinkedList<BTreeLeafNode> leaves, TimingModule tm, SplitCounterModule sm) {
        int count = 0;
        bt = new BTree(this.order, tm, sm);
        BTreeNode preNode = new BTreeLeafNode(this.order, counter);
        BTreeInnerNode root = new BTreeInnerNode(this.order, counter);
        for (BTreeLeafNode leaf : leaves) {
            ++count;
            if (count == 1) {
                BTreeInnerNode parent = root;
                counter.increaseHeightCount();
                parent.setChild(0, leaf);
                leaf.setParent(parent);
                preNode = leaf;
            } else {
                leaf.leftSibling = preNode;
                preNode.rightSibling = leaf;
                try {
                    BTreeInnerNode parent = root.getRightMostChildTest();
                    System.out.println(parent == null);
                    int index = parent.getKeyCount();
                    // System.out.println("count: = " + count + "index: = " + index + " ");
                    System.out.println("The node is");
                      parent.print();
                    parent.setKey(index, leaf.getKey(0));
                    parent.setChild(index+1, leaf);
                    preNode = leaf;
                    //  parent.print();
                    if (parent.isOverflow()) {
                        root = (BTreeInnerNode) parent.dealOverflow();
                    }
                    bt.setHeight(counter.getHeightCount());
                    bt.setRoot(root);
                } catch (UnsupportedGenericException e) {
                    e.printStackTrace();
                }
            }
        }
        bt.setRoot(root);
        return bt;
    }

    public static void main(String[] args) throws CloneNotSupportedException, UnsupportedGenericException {
        List<Double> Keys = new LinkedList<Double>();
        for (int i = 0; i < 120; i++) {
            Keys.add((double) i);
        }

        TimingModule tm = TimingModule.createNew();
        SplitCounterModule sm = SplitCounterModule.createNew();
        testCreateLeaves_dataPages testCase = new testCreateLeaves_dataPages();
        LinkedList<BTreeLeafNode> leaves = testCase.createLeaves(Keys);
        final BTree bt = testCase.createTreeWithBulkLoading(leaves, tm, sm);
//        System.out.println("The btree is");
//        bt.printBtree();
//        final BTree bt = new BTree(4, tm, sm);
        long startTime;
//        final BTree bt = testCase.createTreeWithBulkLoading(leaves, tm, sm);
//        final BTree bt = new BTree(4, tm, sm);
//        for (int i = 0; i < 100; ++i) {
//            bt.insert((double) i, (double) i);
//        }
        bt.printBtree();
        BulkLoader bulkLoader = new BulkLoader(4, tm, sm);
//        for (int i = 0; i < 60; ++i) {
//            bt.insert((double) i, (double) i);
//        }
        final BTree newBTree = bulkLoader.createTreeWithBulkLoading(bt);
//        bt.clearPayload();
//        bt.insert((double) 0, (double) 0);
//        bt.insert((double) 30, (double) 30);
//        for (int i = 0; i <= 100; ++i) {
//            bt.insert((double)i, (double) i);
//        }
//        bt.printBtree();
//        bt.clearPayload();
//        bt.insert((double) 0, (double) 0);
//        bt.insert((double)50, (double) 50);
//        bt.insert((double) 2, (double) 2);
//        bt.printBtree();
        newBTree.printBtree();
        newBTree.setTemplateMode();
        for (int i = 0; i < 60; ++i) {
            newBTree.insert((double) i, (double) i);
            newBTree.printBtree();
        }
        newBTree.printBtree();
        BTreeLeafNode leftMostLeave = newBTree.getLeftMostLeaf();
        while (leftMostLeave != null) {
            leftMostLeave.print();
            leftMostLeave = (BTreeLeafNode) leftMostLeave.rightSibling;
        }
//        BTreeNode leftLeaf = bt.findLeafNodeShouldContainKeyInTemplate((double) 2);
//        leftLeaf.rightSibling.print();
//        leftLeaf.print();
//        BTreeNode rightLeaf = bt.findLeafNodeShouldContainKeyInTemplate((double) 100);
//        while (leftLeaf != rightLeaf) {
//            leftLeaf = leftLeaf.rightSibling;
//        }
//        System.out.println("finished");
//        for (int i = 0; i < 50; ++i) {
//            System.out.println(bt.searchRange((double) i, (double) 50));
//        }
//        for (int i = 0; i < 2; ++i) {
//            new Thread(new Runnable() {
//                public void run() {
//                    while (true) {
//                        System.out.println("The number of count is " + ++count);
//                        bt.searchRange((double) 0, (double) 50);
                    }
//                }
//            }).start();
//        }
//        bt.printBtree();
//        System.out.println(bt.searchRange((double) 50, (double) 60));
//        bt = new BTree(4, tm, sm);
//        bt.printBtree();

//        final int offset = 0;
//        for (int i = 0; i < 2; ++i) {
//            new Thread(new Runnable() {
//                public void run() {
//                    int i = 0;
//                    while (true) {
//                        System.out.println(bt.getHeight());
//                        try {
//                            bt.insert((double) i++, offset);
//                        } catch (UnsupportedGenericException e) {
//                            e.printStackTrace();
//                        }
//                    }
//                }
//            }).start();
//        }
//            new Thread(new Runnable() {
//                public void run() {
//                    while (true) {
//                        try {
//                            Thread.sleep(1);
//                        } catch (InterruptedException e) {
//                            e.printStackTrace();
//                        }
//                        bt.search((double) 0);
//        System.out.println(bt.search((double) 1000));
//                    }
//                }
//            }).start();
//            new Thread(new Runnable() {
//                public void run() {
//                    while (true) {
//        for (int i = 0; i < 6; ++i) {
//            try {
//                bt.insert((double) i, (double) i);
//                System.out.println("After insert, the btree is : ");
//                bt.printBtree();
//                System.out.println();
//            } catch (UnsupportedGenericException e) {
//                e.printStackTrace();
//            }
//        }
//        System.out.println("Hello");
//        for (int i = 0; i < 8; ++i) {
//            System.out.println(bt.searchRange((double) 0, (double) i));
//        }
//        System.out.println("Hello");
//        System.out.println(bt.searchRange((double) 2, (double) 50));
//        for (int i = 60; i >= 0; --i) {
//            try {
//                bt.delete((double) i);
//                System.out.println("After delete, the btree is : ");
//                bt.printBtree();
//                System.out.println();
//            } catch (UnsupportedGenericException e) {
//                e.printStackTrace();
//            }
//        }

//                    }
//                }
//            }).start();

//    }
//    }
}
