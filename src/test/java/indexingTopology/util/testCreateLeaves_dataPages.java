package indexingTopology.util;

import backtype.storm.tuple.Tuple;
import indexingTopology.DataSchema;
import indexingTopology.exception.UnsupportedGenericException;
import javafx.util.Pair;

import java.io.IOException;
import java.math.BigInteger;
import java.util.*;

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
            Pair pair = new Pair(key, offset++);
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
            if (leaf.isOverflowIntemplate()) {
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
        if (leaf.isOverflowIntemplate()) {
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
                    BTreeInnerNode parent = root.getRightMostChild();
                    int index = parent.getKeyCount();
                    // System.out.println("count: = " + count + "index: = " + index + " ");
                    //  parent.print();
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
     /*   double[] list = {103.657223,
        103.6610031, 103.6809998,103.71283,
        103.717561 ,103.721557 ,103.726143 ,103.727173,
        103.732578, 103.734946, 103.747694, 103.748366,
        103.759505 ,103.761742, 103.764019 ,103.764215,
        103.764452, 103.765004 ,103.768202, 103.769366,
        103.795, 103.797702, 103.798689, 103.8000031,
        103.80102, 103.802002, 103.802296, 103.802809,
        103.803, 103.8030014, 103.804, 103.805881,
        103.806, 103.806951, 103.808, 103.81,
        103.812, 103.813, 103.816, 103.835,
        103.837317, 103.838, 103.841, 103.8410034,
        103.844, 103.8440018, 103.8450012, 103.846001,
        103.847, 103.849, 103.849023, 103.85,
        103.852, 103.857, 103.86, 103.861291,
        103.862999, 103.864, 103.866659, 103.867225,
        103.87, 103.872842, 103.876432, 103.878438,
        103.878443, 103.878498, 103.881098, 103.881132,
        103.881307, 103.883774, 103.884086, 103.884123,
        103.886768, 103.887919, 103.890312, 103.8929977,
        103.89488, 103.89835, 103.899422, 103.899906,
        103.904, 103.912186, 103.914003, 103.9160004,
        103.917, 103.917269, 103.923289, 103.93,
        103.933393, 103.933779, 103.93506, 103.936089,
        103.941531, 103.942957, 103.946261, 103.9489975,
        103.9509964, 103.9540024, 103.9560013, 103.956213,
        103.958248, 103.9609985, 103.961526, 103.9629974,
        103.963071, 103.964748,103.96688, 103.96769, 103.968, 103.97, 103.9850006, 103.986};*/
        List<Double> Keys = new LinkedList<Double>();
        for (int i = 0; i <= 100; ++i) {
            Keys.add((double) i);
        }
//        Keys.add(10);
//        Keys.add(10);
//        Keys.add(10);
//        Keys.add(11);
//        Keys.add(12);
//        Keys.add(13);
    /*    Keys.add(3);
        Keys.add(4);
        Keys.add(6);
        Keys.add(9);
        Keys.add(10);
        Keys.add(11);
        Keys.add(12);
        Keys.add(13);
        Keys.add(20);
        Keys.add(22);
        Keys.add(23);
        Keys.add(31);
        Keys.add(35);
        Keys.add(36);
        Keys.add(38);
        Keys.add(41);
        Keys.add(44);
        Keys.add(50);
        Keys.add(50);
        Keys.add(50);
        Keys.add(50);*/
      /*  List<Double> Keys = new LinkedList<Double>();
        for (int i = 0; i < list.length; ++i) {
            Keys.add(list[i]);
        }*/

     /*   for (Double key : Keys) {
            System.out.println(key);
        }*/
        testCreateLeaves_dataPages testCase = new testCreateLeaves_dataPages<Double, Integer>();
        LinkedList<BTreeLeafNode> leaves = testCase.createLeaves(Keys);
        //   for (BTreeLeafNode leaf : leaves) {
        //      leaf.print();
        //   }

        TimingModule tm = TimingModule.createNew();
        SplitCounterModule sm = SplitCounterModule.createNew();
//        final BTree bt = createTreeWithBulkLoading(leaves);
        long startTime;

        final BTree bt = testCase.createTreeWithBulkLoading(leaves, tm, sm);
//        bt.clearPayload();
//        bt.insert((double) 0, (double) 0);
//        bt.insert((double) 30, (double) 30);
        bt.printBtree();
        int count = 0;
//        System.out.println(bt.searchRange((double) 0, (double) 50));
//        for (int i = 0; i < 2; ++i) {
//            new Thread(new Runnable() {
//                public void run() {
                    while (true) {
//                        System.out.println("The number of count is " + ++count);
                        bt.searchRange((double) 0, (double) 50);
                    }
//                }
//            }).start();
//        }
//        bt.printBtree();
//        System.out.println(bt.searchRange((double) 50, (double) 60));
//        bt = new BTree(4, tm, sm);
//        bt.printBtree();
//        bt.clearPayload();
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
    }
}
