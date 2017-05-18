package indexingTopology.util;

import indexingTopology.exception.UnsupportedGenericException;

import java.io.*;
import java.util.*;
import java.util.concurrent.atomic.AtomicLong;

public class BTreeLeafNode<TKey extends Comparable<TKey>> extends BTreeNode<TKey> implements Serializable {
    protected ArrayList<ArrayList<byte []>> tuples;
    protected ArrayList<ArrayList<Integer>> offsets;
    protected AtomicLong atomicKeyCount;

    public BTreeLeafNode(int order) {
        super(order);
        this.keys = new ArrayList<>(order);
        this.tuples = new ArrayList<>(order + 1);
        this.offsets = new ArrayList<>(order + 1);
        atomicKeyCount = new AtomicLong(0);
    }

    public boolean validateParentReference() {
        return true;
    }

    public boolean validateNoDuplicatedChildReference() {
        return true;
    }

    public boolean validateAllLockReleased() {
        return true;
    }

    public int getDepth() {
        return 1;
    }


    public ArrayList<Integer> getOffsets(int index) {
        ArrayList<Integer> offsets;
        offsets = this.offsets.get(index);
        return offsets;
    }

    public void setTupleList(int index, ArrayList<byte[]> tuples) {
        this.atomicKeyCount.addAndGet(tuples.size());
        if (index < this.tuples.size())
            this.tuples.set(index, tuples);
        else if (index == this.tuples.size()) {
            this.tuples.add(index, tuples);
        } else
            throw new ArrayIndexOutOfBoundsException("index out of bounds");
    }

    public void setOffsetList(int index, ArrayList<Integer> offsets) {
        if (index < this.offsets.size())
            this.offsets.set(index, offsets);
        else if (index == this.offsets.size()) {
            this.offsets.add(index, offsets);
        } else
            throw new ArrayIndexOutOfBoundsException("index out of bounds");
    }


    @Override
    public TreeNodeType getNodeType() {
        return TreeNodeType.LeafNode;
    }


    @Override
    public int search(TKey key) {
        int low = 0;
        int high = this.getKeyCount() - 1;
        while (low <= high) {
            int mid = (low + high) >> 1;
            int cmp = this.getKey(mid).compareTo(key);
            if (cmp == 0) {
                return mid;
            } else if (cmp > 0) {
                high = mid - 1;
            } else {
                low = mid + 1;
            }
        }
        return -1;
    }

    private int searchMinIndex(TKey key) {
        int low = 0;
        int high = this.getKeyCount() - 1;
        while (low <= high) {
            int mid = (low + high) >> 1;
            int cmp = this.getKey(mid).compareTo(key);
            if (cmp == 0) {
                return mid;
            } else if (cmp > 0) {
                high = mid - 1;
            } else {
                low = mid + 1;
            }
        }
        return low;
    }

    private int searchLargestIndex(TKey key) {
        int low = 0;
        int high = this.getKeyCount() - 1;
        while (low <= high) {
            int mid = (low + high) >> 1;
            int cmp = this.getKey(mid).compareTo(key);
            if (cmp == 0) {
                return mid;
            } else if (cmp > 0) {
                high = mid - 1;
            } else {
                low = mid + 1;
            }
        }
        return high;
    }

    public BTreeNode insertKeyTuples(TKey key, byte[] serilizedTuple, boolean templateMode) throws UnsupportedGenericException{
        BTreeNode node = null;

        int index = searchMinIndex(key);

        if (!(index < this.keys.size() && this.getKey(index).compareTo(key) == 0)) {
            this.keys.add(index, key);
            this.tuples.add(index, new ArrayList<byte[]>());
            this.offsets.add(index, new ArrayList<Integer>());
            ++this.keyCount;
        }

        atomicKeyCount.incrementAndGet();
        this.tuples.get(index).add(serilizedTuple);
        this.offsets.get(index).add(serilizedTuple.length);

        if (!templateMode && isOverflow()) {
            node = dealOverflow();
        }

        return node;
    }


    /**
     * When splits a leaf node, the middle key is kept on new node and be pushed to parent node.
     */
    @Override
    protected BTreeNode<TKey> split() {
        BTreeLeafNode newRNode = new BTreeLeafNode(this.ORDER);

        int midIndex = this.getKeyCount() / 2;

        for (int i = midIndex; i < this.getKeyCount(); ++i) {
            try {
                newRNode.setKey(i - midIndex, this.getKey(i));
            } catch (UnsupportedGenericException e) {
                e.printStackTrace();
            }

            newRNode.setTupleList(i - midIndex, this.getTuplesWithSpecificIndex(i));
            newRNode.setOffsetList(i - midIndex, this.getOffsets(i));

//            this.atomicKeyCount.addAndGet(-this.getTuplesWithSpecificIndex(i).size());
            this.atomicKeyCount.decrementAndGet();
        }

        newRNode.keyCount = this.getKeyCount() - midIndex;

        for (int i = this.getKeyCount() - 1; i >= midIndex; i--)
            this.deleteAt(i);
        this.keyCount = midIndex;

        return newRNode;
    }


    @Override
    protected BTreeNode<TKey> pushUpKey(TKey key, BTreeNode<TKey> leftChild, BTreeNode<TKey> rightNode) {
        throw new UnsupportedOperationException();
    }

    private void deleteAt(int index) {
        this.keys.remove(index);

        this.tuples.remove(index);

        this.offsets.remove(index);
        --this.keyCount;
    }

    protected void clearNode() {
        this.keys.clear();

        this.tuples.clear();

        this.offsets.clear();

        this.keyCount = 0;

        atomicKeyCount.set(0);
    }

    @Override
    public BTreeNode deepCopy(List<BTreeNode> nodes) {
        BTreeLeafNode node = new BTreeLeafNode(ORDER);
//        node.keyCount = keyCount;

//        node.keys = (ArrayList) keys.getTemplate();
//        node.tuples = (ArrayList) tuples.getTemplate();
//        node.offsets = (ArrayList) offsets.getTemplate();
//        node.atomicKeyCount.set(this.atomicKeyCount.get());
        nodes.add(node);
        return node;
    }



    public void setKeys(ArrayList<TKey> keys) {
        this.keys = keys;
    }

    public void setTuples(ArrayList<ArrayList<byte[]>> tuples) {
        this.tuples = tuples;
    }

    public long getAtomicKeyCount() {
        return atomicKeyCount.get();
    }

    public ArrayList<byte[]> getTuplesWithSpecificIndex(int index) {
        if (index < getKeyCount()) {
            ArrayList<byte[]> tuples;
            tuples = this.tuples.get(index);
            return tuples;
        }
        return new ArrayList<>();
    }

    /* The code below is used to support search operation.*/
    @SuppressWarnings("unchecked")
    public ArrayList<byte[]> getTuplesWithinKeyRange(TKey leftKey, TKey rightKey) {
        ArrayList<byte[]> tuples = new ArrayList<>();

        int startIndex = searchMinIndex(leftKey);
        int endIndex = searchLargestIndex(rightKey);

        for (int index = startIndex; index <= endIndex; ++index) {
            if (keys.get(index).compareTo(leftKey) >=0 && keys.get(index).compareTo(rightKey) <=0) {
                tuples.addAll(getTuplesWithSpecificIndex(index));
            }
        }

        return tuples;
    }
}