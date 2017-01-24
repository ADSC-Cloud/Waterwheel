package indexingTopology.util;

import indexingTopology.exception.UnsupportedGenericException;

import java.util.*;


/**
 * Created by acelzj on 7/15/16.
 */
public class TemplateUpdater<TKey extends Comparable<TKey>> {

    private int order;

    private BTree<Double, Integer> template;

    public TemplateUpdater(int btreeOrder) {
        order = btreeOrder;
    }

    @SuppressWarnings("unchecked")
    private List<BTreeInnerNode> createInnerNodes(BTree oldBTree) {
        BTreeLeafNode currentLeave = oldBTree.getLeftMostLeaf();

        BTreeLeafNode copyOfCurrentLeave = currentLeave;

        int totalKeyCount = 0;
        int numberOfLeaves = 0;

        while (currentLeave != null) {
            ++numberOfLeaves;
            totalKeyCount += currentLeave.getKeyCount();
            currentLeave = (BTreeLeafNode) currentLeave.rightSibling;
        }

        int averageKeyCount = totalKeyCount / numberOfLeaves;

        ArrayList<BTreeLeafNode> leaves = createLeaves(copyOfCurrentLeave, averageKeyCount, numberOfLeaves, totalKeyCount);

        List<BTreeInnerNode> innerNodes = new ArrayList<BTreeInnerNode>();
        BTreeInnerNode node = new BTreeInnerNode(order);
        int index = 0;
        BTreeLeafNode preChild = leaves.get(index);
        node.setChild(0, preChild);
        ++index;

        while (index < leaves.size()) {
            BTreeLeafNode child = leaves.get(index);
            if (node.isSafe()) {
                node.insertKey(child.getKey(0));
                node.setChild(node.getKeyCount(), child);
                setSiblingsOfChild(preChild, child);
                preChild = child;
            } else {
                setSiblingsOfChild(preChild, child);
                preChild = child;
                innerNodes.add(node);
                node = new BTreeInnerNode(order);
                child = leaves.get(index);
                node.setChild(0, child);
            }
            ++index;
        }

        innerNodes.add(node);

        return innerNodes;
    }



    private void setSiblingsOfChild(BTreeLeafNode prechild, BTreeLeafNode child) {
        child.leftSibling = prechild;
        prechild.rightSibling = child;
    }


    /**
     * use an optimized bulk loading to build a new B+ tree.
     * @param oldBTree, requires that the height of oldBTree > 1
     * @return a new BTree
     */

    @SuppressWarnings("unchecked")
    public BTree<Double, Integer> createTreeWithBulkLoading(BTree oldBTree) {
        List<BTreeInnerNode> innerNodes = createInnerNodes(oldBTree);

        int count = 0;
        template = new BTree(this.order);
        BTreeInnerNode root = new BTreeInnerNode(this.order);
        for (BTreeInnerNode node : innerNodes) {
            ++count;
            if (count == 1) {
                BTreeInnerNode parent = root;
                parent.setChild(0, node);
                node.setParent(parent);
                template.setRoot(root);
            } else {
                try {
                    BTreeInnerNode parent = root.getRightMostChild();
                    int index = parent.getKeyCount();
                    parent.setKey(index, node.getChild(0).getKey(0));
                    parent.setChild(index+1, node);
                    if (parent.isOverflow()) {
                        root = (BTreeInnerNode) parent.dealOverflow();
                    }
//                    template.setHeight(counter.getHeightCount());
                    template.setRoot(root);
                } catch (UnsupportedGenericException e) {
                    e.printStackTrace();
                }
            }
        }

        template.setRoot(root);
        template.setTemplateMode();
        return template;
    }

    public ArrayList<BTreeLeafNode> createLeaves(BTreeLeafNode mostLeftLeaf, int averageKeyCount, int numberOfLeaves, int totalKeyCount) {
        BTreeLeafNode currentLeaf = mostLeftLeaf;

        ArrayList<BTreeLeafNode> leaves = new ArrayList<>(numberOfLeaves);

        int indexInCurrentLeaf = 0;

        BTreeLeafNode preNode = null;

        //This variable means the max index of the leaf node which should have one more key.
        int maxIndex = totalKeyCount - (numberOfLeaves) * averageKeyCount;

        for (int i = 0; i < numberOfLeaves; ++i) {

            int keyCount = i < maxIndex ? averageKeyCount + 1 : averageKeyCount;

            BTreeLeafNode leaf = new BTreeLeafNode(keyCount);

            if (i == 0) {
                preNode = leaf;
            } else {
                leaf.leftSibling = preNode;
                preNode.rightSibling = leaf;
                preNode = leaf;
            }

            while (leaf.getKeyCount() < keyCount && currentLeaf != null) {
                if (indexInCurrentLeaf == currentLeaf.getKeyCount()) {
                    currentLeaf = (BTreeLeafNode) currentLeaf.rightSibling;
                    indexInCurrentLeaf = 0;
                }

                if (currentLeaf != null) {
                    insertTuplesIntoLeaf(currentLeaf, leaf, indexInCurrentLeaf, leaf.getKeyCount());
                    ++indexInCurrentLeaf;
                }
            }

            leaves.add(leaf);
        }

        return leaves;
    }

    @SuppressWarnings("unchecked")
    private void insertTuplesIntoLeaf(BTreeLeafNode currentLeaf, BTreeLeafNode leaf, int index, int indexOfKey) {
        leaf.keys.add(currentLeaf.getKey(index));
        leaf.tuples.add(new ArrayList<byte[]>());
        leaf.offsets.add(new ArrayList<Integer>());

        try {
            leaf.bytesCount += UtilGenerics.sizeOf(currentLeaf.getKey(index).getClass());
        } catch (UnsupportedGenericException e) {
            e.printStackTrace();
        }

        ArrayList<byte[]> tuples = currentLeaf.getTuples(index);
        ArrayList<Integer> offsets = currentLeaf.getOffsets(index);

        ((ArrayList) leaf.tuples.get(indexOfKey)).addAll(tuples);
        leaf.tupleCount.addAndGet(tuples.size());

        ((ArrayList) leaf.offsets.get(indexOfKey)).addAll(offsets);

        for (int i = 0; i < tuples.size(); ++i) {
            leaf.bytesCount += tuples.get(i).length;
            leaf.bytesCount += (Integer.SIZE / Byte.SIZE);
        }

    }

}