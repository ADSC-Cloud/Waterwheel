package indexingTopology.util;

import indexingTopology.exception.UnsupportedGenericException;

import java.io.*;
import java.util.*;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Lock;

public class BTreeLeafNode<TKey extends Comparable<TKey>> extends BTreeNode<TKey> implements Serializable {
    protected ArrayList<ArrayList<byte []>> tuples;
    protected ArrayList<ArrayList<Integer>> offsets;
    protected AtomicLong tupleCount;

    public BTreeLeafNode(int order) {
        super(order);
        this.keys = new ArrayList<>(order);
        this.tuples = new ArrayList<>(order + 1);
        this.offsets = new ArrayList<>(order + 1);
        tupleCount = new AtomicLong(0);
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

    public ArrayList<byte[]> getTuples(int index) {
        if (index < getKeyCount()) {
            ArrayList<byte[]> tuples;
            tuples = this.tuples.get(index);
            return tuples;
        }
        return null;
    }

    public ArrayList<Integer> getOffsets(int index) {
        ArrayList<Integer> offsets;
        offsets = this.offsets.get(index);
        return offsets;
    }

    public void setTupleList(int index, ArrayList<byte[]> tuples) {
        this.tupleCount.addAndGet(tuples.size());
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

    private int searchIndex(TKey key) {
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

    public BTreeNode insertKeyTuples(TKey key, byte[] serilizedTuple, boolean templateMode) throws UnsupportedGenericException{
        BTreeNode node = null;

        int index = searchIndex(key);

        if (!(index < this.keys.size() && this.getKey(index).compareTo(key) == 0)) {
            this.keys.add(index, key);
            this.tuples.add(index, new ArrayList<byte[]>());
            this.offsets.add(index, new ArrayList<Integer>());
            ++this.keyCount;
        }

        tupleCount.incrementAndGet();
        this.tuples.get(index).add(serilizedTuple);
        this.offsets.get(index).add(serilizedTuple.length);

        if (!templateMode && isOverflow()) {
            node = dealOverflow();
        }

        return node;
    }


    public BTreeNode insertKeyTuples(TKey key, byte[] serilizedTuple, boolean templateMode, Counter counter) throws UnsupportedGenericException{
        BTreeNode node = null;

        int index = searchIndex(key);

        if (keys.contains(key)) {
            counter.addCount();
        }

        if (!(index < this.keys.size() && this.getKey(index).compareTo(key) == 0)) {
            this.keys.add(index, key);
            this.tuples.add(index, new ArrayList<byte[]>());
            this.offsets.add(index, new ArrayList<Integer>());
            ++this.keyCount;
        }


        tupleCount.incrementAndGet();
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
            newRNode.setTupleList(i - midIndex, this.getTuples(i));

            this.tupleCount.addAndGet(-this.getTuples(i).size());

            newRNode.setOffsetList(i - midIndex, this.getOffsets(i));
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

        tupleCount.set(0);
    }

    @Override
    public BTreeNode deepCopy(List<BTreeNode> nodes) {
        BTreeLeafNode node = new BTreeLeafNode(ORDER);
        node.keyCount = keyCount;

        node.keys = (ArrayList) keys.clone();

        node.tuples = (ArrayList) tuples.clone();

        node.offsets = (ArrayList) offsets.clone();

        node.tupleCount.set(this.tupleCount.get());

        nodes.add(node);
        return node;
    }


    /* The code below is used to support search operation.*/
    public List<byte[]> search(TKey leftKey, TKey rightKey){
        // find first index satisfying range
        Lock lastLock = this.getrLock();
        int firstIndex = searchIndex(leftKey);
        List<byte[]> retList = new ArrayList<byte[]>();
        BTreeLeafNode currLeaf = this;
        BTreeNode currRightSibling = this;
        BTreeNode tmpNode = this;

        int currIndex = firstIndex;

        assert currLeaf.lock.getReadLockCount() > 0;

        // case when all keys in the node are smaller than leftKey - shift to next rightSibling
        if (firstIndex >= this.getKeyCount()) {
            currLeaf = (BTreeLeafNode) this.rightSibling;
            if (currLeaf != null) {
                tmpNode = this;
                currRightSibling = this.rightSibling;
                currLeaf.acquireReadLock();
                if (lastLock != null) {
                    lastLock.unlock();
                }
                lastLock = currLeaf.getrLock();

                while (currRightSibling != currLeaf) {
                    currRightSibling = tmpNode.rightSibling;

                    lastLock.unlock();
                    currLeaf = (BTreeLeafNode) tmpNode.rightSibling;

                    currLeaf.acquireReadLock();
                    lastLock = currLeaf.getrLock();
                }
            }

            while (currLeaf != null && currLeaf.getKeyCount() == 0) {
                if (currLeaf.rightSibling != null) {
                    currLeaf.rightSibling.acquireReadLock();
                    lastLock.unlock();
                    lastLock = currLeaf.rightSibling.getrLock();
                    currLeaf = (BTreeLeafNode) currLeaf.rightSibling;
                }
            }

            currIndex = 0;
        }


        while (currLeaf != null && currLeaf.getKeyCount() > 0 && currLeaf.getKey(currIndex).compareTo(rightKey) <= 0) {
            assert currLeaf.lock.getReadLockCount() > 0;
//            System.out.println("key count" + currLeaf.getKeyCount());
//            System.out.println(currLeaf == null);
            retList.addAll(currLeaf.getTuples(currIndex));
            currIndex++;
            if (currIndex >= currLeaf.getKeyCount()) {

                if (currLeaf.rightSibling != null) {

                    tmpNode = currLeaf;

                    currRightSibling = currLeaf.rightSibling;

                    currRightSibling.acquireReadLock();
                    if (lastLock != null) {
                        lastLock.unlock();
                    }

                    lastLock = currRightSibling.getrLock();

                    currLeaf = (BTreeLeafNode) tmpNode.rightSibling;

                    while (currLeaf != null && currRightSibling != null && currRightSibling != currLeaf) {
                        currRightSibling = tmpNode.rightSibling;

                        lastLock.unlock();
                        currLeaf = (BTreeLeafNode) tmpNode.rightSibling;

                        currLeaf.acquireReadLock();
                        lastLock = currLeaf.getrLock();
                    }

                    while (currLeaf != null && currLeaf.getKeyCount() == 0) {
                        if (currLeaf.rightSibling != null) {
                            currLeaf.rightSibling.acquireReadLock();
                            lastLock.unlock();
                            lastLock = currLeaf.rightSibling.getrLock();
                            currLeaf = (BTreeLeafNode) currLeaf.rightSibling;
                        }
                    }

                } else {
                    break;
                }
                if (currLeaf != null) {
                    assert currLeaf.lock.getReadLockCount() > 0;
                }
                currIndex = 0;
            }
        }

        if (lastLock != null) {
            lastLock.unlock();
        }

        return retList;
    }

    public long getTupleCount() {
        return tupleCount.get();
    }

    public void setKeys(ArrayList<TKey> keys) {
        this.keys = keys;
    }

    public void setTuples(ArrayList<ArrayList<byte[]>> tuples) {
        this.tuples = tuples;
    }

    @SuppressWarnings("unchecked")
    public ArrayList<byte[]> getTuples(TKey leftKey, TKey rightKey) {

        ArrayList<byte[]> tuples = new ArrayList<>();

        Double leftKeyInDouble = (Double) leftKey;

        Double rightKeyInDouble = (Double) rightKey;

        for (Double key = leftKeyInDouble; key <= rightKeyInDouble; ++key) {
            int index = search((TKey) key);
            if (index != -1 && index < getKeyCount()) {
                tuples.addAll(getTuples(index));
            }
        }

        return tuples;
    }
}