package indexingTopology.util;

import backtype.storm.tuple.Tuple;
import indexingTopology.DataSchema;
import indexingTopology.exception.UnsupportedGenericException;

import java.io.IOException;
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

    private TreeMap<TKey, Integer> record;

    public testCreateLeaves_dataPages() {
        this.order = 4;
    }

    public TreeMap createRecord(List<TKey> keys) {
        HashMap<TKey, Integer> record = new HashMap<TKey, Integer>();
        int offset = 0;
        for (TKey key : keys) {
            offset += 1;
            record.put(key, offset);
        }
        this.record = new TreeMap<TKey, Integer>(record);
        return this.record;
    }
    public LinkedList<BTreeLeafNode> createLeaves(List<TKey> keys) {
        createRecord(keys);
        TreeMap<TKey, Integer> treeMap = new TreeMap<TKey, Integer>(record);
        BytesCounter counter = new BytesCounter();
        BTreeLeafNode leaf = new BTreeLeafNode(order, counter);
        LinkedList<BTreeLeafNode> leaves = new LinkedList<BTreeLeafNode>();
        BTreeLeafNode lastLeaf = leaf;
        TKey lastKey = treeMap.firstKey();
        int count = 0;
        for (TKey key : treeMap.keySet()) {
            System.out.println(key);
            if (leaf.isOverflowIntemplate()) {
                leaf.delete(lastKey);
                leaves.add(leaf);
                leaf = new BTreeLeafNode(order, counter);
                try {
                    leaf.insertKeyValue(lastKey, treeMap.get(lastKey));
                    leaf.insertKeyValue(key, treeMap.get(key));
                } catch (UnsupportedGenericException e) {
                    e.printStackTrace();
                }
            } else {
                try {
                    leaf.insertKeyValue(key, treeMap.get(key));
                    lastKey = key;
                } catch (UnsupportedGenericException e) {
                    e.printStackTrace();
                }
            }
        }
        if (leaf.isOverflowIntemplate()) {
            leaf.keys.remove(lastKey);
            leaf.values.remove(lastKey);
            leaves.add(leaf);
            leaf = new BTreeLeafNode(order, counter);
            try {
                leaf.insertKeyValue(lastKey, treeMap.get(lastKey));
            } catch (UnsupportedGenericException e) {
                e.printStackTrace();
            }
            leaves.add(leaf);
        } else {
            leaves.add(leaf);
        }
        return leaves;
    }

    public void checkNewTree(SplitCounterModule sm) {
        double numberOfKeys = 0;
        for (TKey key : record.keySet()) {
            ++numberOfKeys;
            try {
                bt.insert(key, record.get(key));
            } catch (UnsupportedGenericException e) {
                e.printStackTrace();
            }
        }
        double percentage = (double) sm.getCounter() / numberOfKeys;
        System.out.println(percentage);
    }

    public BTree createTreeWithBulkLoading(LinkedList<BTreeLeafNode> leaves, TimingModule tm, SplitCounterModule sm) {
        int count = 0;
        BytesCounter counter = new BytesCounter();
        bt = new BTree(this.order, tm, sm);
        BTreeNode preNode = new BTreeLeafNode(this.order, counter);
        BTreeInnerNode root = new BTreeInnerNode(this.order, counter);
        for (BTreeLeafNode leaf : leaves) {
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
                    System.out.println("count: = " + count + "index: = " + index + " ");
                  //  parent.print();
                    parent.setKey(index, leaf.getKey(0));
                    parent.setChild(index+1, leaf);
                    preNode = leaf;
                    parent.print();
                    if (parent.isOverflow()) {
                        root = (BTreeInnerNode) parent.dealOverflow(sm, leaf);
                    }
                    bt.setRoot(root);
                    bt.printBtree();
                } catch (UnsupportedGenericException e) {
                    e.printStackTrace();
                }
            }
        }
        bt.setRoot(root);
        return bt;
    }

    public static void main(String[] args) {
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
        List<Integer> Keys = new LinkedList<Integer>();
        for (int i = 0; i <= 10; ++i) {
            Keys.add(i);
        }
        Keys.add(10);
        Keys.add(10);
        Keys.add(10);
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
        for (BTreeLeafNode leaf : leaves) {
            leaf.print();
        }
        TimingModule tm = TimingModule.createNew();
        SplitCounterModule sm = SplitCounterModule.createNew();
        BTree bt = testCase.createTreeWithBulkLoading(leaves, tm, sm);
        bt.printBtree();
        bt.clearPayload();
        testCase.checkNewTree(new SplitCounterModule());
        bt = new BTree(4, tm, sm);
        int offset = 0;
        for (int i = 0; i <= 10; ++i) {
            try {
                bt.insert(i, ++offset);
            } catch (UnsupportedGenericException e) {
                e.printStackTrace();
            }
        }
        try {
            bt.insert(10, ++offset);
        } catch (UnsupportedGenericException e) {
            e.printStackTrace();
        }

        try {
            bt.insert(10, ++offset);
        } catch (UnsupportedGenericException e) {
            e.printStackTrace();
        }
        try {
            bt.insert(10, ++offset);
        } catch (UnsupportedGenericException e) {
            e.printStackTrace();
        }

        bt.printBtree();

     //   TreeMap record = testCase.createRecord(Keys);

    }
}
