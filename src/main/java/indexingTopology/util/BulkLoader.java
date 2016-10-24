package indexingTopology.util;

import backtype.storm.tuple.Tuple;
import clojure.lang.Ref;
import indexingTopology.DataSchema;
import indexingTopology.exception.UnsupportedGenericException;
import javafx.util.Pair;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Semaphore;


/**
 * Created by acelzj on 7/15/16.
 */
public class BulkLoader <TKey extends Comparable<TKey>, TValue> {

    private int order;

    private BTree bt;

    private List<Pair<TKey, TValue>> record;

    private TimingModule tm;

    private SplitCounterModule sm;

    private int numberOfLeaves;

    private BytesCounter counter;

    private boolean templateMode;


    public BulkLoader(int btreeOrder, TimingModule tm, SplitCounterModule sm) {
        order = btreeOrder;
        this.tm = tm;
        this.sm = sm;
        templateMode = false;
//        record = new TreeMap<TKey, TValue>();
        record = new ArrayList<Pair<TKey, TValue>>();
        counter = new BytesCounter();
        counter.increaseHeightCount();
    }

    public synchronized void addRecord(Pair pair) {
        record.add(pair);
    }

    public synchronized List<TValue> pointSearch(TKey key) {
        List<TValue> list = new ArrayList<TValue>();
        for(Pair<TKey, TValue> pair: record) {
            if(pair.getKey().equals(key)) {
                list.add(pair.getValue());
            }
        }
        return list;
    }

    public int getNumberOfRecord() {
        return record.size();
    }
    public void resetRecord() {
        record.clear();
    }

    public LinkedList<BTreeLeafNode> createLeaves() {
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
/*
    public BTree createTreeWithBulkLoading() {
        LinkedList<BTreeLeafNode> leaves = createLeaves();
        //    for (BTreeLeafNode leaf : leaves) {
        //        leaf.print();
        //    }
        int count = 0;
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
                counter.increaseHeightCount();
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
    }*/

    private List<BTreeInnerNode> createInnerNodes(BTree oldBTree) {
        BTreeLeafNode currentLeave = oldBTree.getLeftMostLeaf();
        List<BTreeInnerNode> innerNodes = new ArrayList<BTreeInnerNode>();
        BTreeInnerNode node = new BTreeInnerNode(order, counter);
        int minIndexOfCurrentNode = 0;
        int count = 1;
        BTreeLeafNode child = null;
        BTreeLeafNode prechild = child;
        while (currentLeave != null) {
            int index = order * count - minIndexOfCurrentNode;
            while (index < currentLeave.getKeyCount()) {
                if (node.isSafe()) {
                    node.insertKey(currentLeave.getKey(index));
                    child = new BTreeLeafNode(order, counter);
                    node.setChild(node.getKeyCount() - 1, child);
                    if (prechild != null) {
                        setSiblingsOfChild(prechild, child);
                    }
                    prechild = child;
                    index += order;
                    ++count;
                } else {
                    child = new BTreeLeafNode(order, counter);
                    setSiblingsOfChild(prechild, child);
                    prechild = child;
                    node.setChild(node.getKeyCount(), child);
                    innerNodes.add(node);
                    node = new BTreeInnerNode(order, counter);
                    node.insertKey(currentLeave.getKey(index));
                    child = new BTreeLeafNode(order, counter);
                    setSiblingsOfChild(prechild, child);
                    prechild = child;
                    node.setChild(node.getKeyCount() - 1, child);
                    index += order;
                    count += 1;
                }
            }
            minIndexOfCurrentNode += currentLeave.getKeyCount();
            currentLeave = (BTreeLeafNode) currentLeave.rightSibling;
        }
        child = new BTreeLeafNode(order, counter);
        setSiblingsOfChild(prechild, child);
        node.setChild(node.getKeyCount(), child);
        innerNodes.add(node);
        return innerNodes;
    }

    private void setSiblingsOfChild(BTreeLeafNode prechild, BTreeLeafNode child) {
        child.leftSibling = prechild;
        prechild.rightSibling = child;
    }
    public BTree createTreeWithBulkLoading(BTree oldBTree) {
        List<BTreeInnerNode> innerNodes = createInnerNodes(oldBTree);
        int count = 0;
        bt = new BTree(this.order, tm, sm);
        BTreeInnerNode preNode = new BTreeInnerNode(this.order, counter);
        BTreeInnerNode root = new BTreeInnerNode(this.order, counter);
        for (BTreeInnerNode node : innerNodes) {
            ++count;
            if (count == 1) {
                BTreeInnerNode parent = root;
                parent.setChild(0, node);
                node.setParent(parent);
                preNode = node;
                counter.increaseHeightCount();
                bt.setRoot(root);
            } else {
                try {
                    BTreeInnerNode parent = root.getRightMostChild();
                    int index = parent.getKeyCount();
                    // System.out.println("count: = " + count + "index: = " + index + " ");
                    parent.setKey(index, node.getKey(0));
                    parent.setChild(index+1, node);
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
    public List<Pair<TKey, TValue>> getRecords() {
        return record;
    }

}