package indexingTopology.util;

import org.apache.storm.tuple.Values;
import indexingTopology.exception.UnsupportedGenericException;
import org.omg.Messaging.SYNC_WITH_TRANSPORT;

import java.io.*;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Lock;

public class BTreeLeafNode<TKey extends Comparable<TKey>, TValue> extends BTreeNode<TKey> implements Serializable {
    protected ArrayList<ArrayList<TValue>> values;
    protected ArrayList<ArrayList<byte []>> tuples;
    protected ArrayList<ArrayList<Integer>> offsets;
    protected int bytesCount;
    protected AtomicLong tupleCount;

    public BTreeLeafNode(int order, BytesCounter counter) {
        super(order,counter);
        this.keys = new ArrayList<TKey>(order);
        this.values = new ArrayList<ArrayList<TValue>>(order + 1);
        this.tuples = new ArrayList<ArrayList<byte []>>(order + 1);
        this.offsets = new ArrayList<ArrayList<Integer>>(order + 1);
        tupleCount = new AtomicLong(0);
        bytesCount = 0;
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

    public BTreeLeafNode(BTreeNode oldNode) throws CloneNotSupportedException{
        super(oldNode.ORDER, (BytesCounter) oldNode.counter.clone());
        this.keys = new ArrayList<TKey>();
        this.keys.addAll(oldNode.keys);
    }

    @SuppressWarnings("unchecked")
    public ArrayList<TValue> getValueList(int index) {
        ArrayList<TValue> values;
        values = this.values.get(index);
        return values;
    }

    public ArrayList<byte[]> getTuples(int index) {
        if (index < getKeyCount()) {
            ArrayList<byte[]> tuples;
            tuples = this.tuples.get(index);
            return tuples;
        }
        return null;
    }

    public ArrayList<byte[]> getTuples() {
        ArrayList<byte[]> tuples = new ArrayList<byte[]>();

        for (int i = 0; i < keys.size(); ++i) {
            tuples.addAll(getTuples(i));
        }

        return tuples;
    }

    public ArrayList<Integer> getOffsets(int index) {
        ArrayList<Integer> offsets;
        offsets = this.offsets.get(index);
        return offsets;
    }

    public void setValueList(int index, ArrayList<TValue> value) {

        if (index < this.values.size())
            this.values.set(index, value);
        else if (index == this.values.size()) {
            this.values.add(index, value);
        } else
            throw new ArrayIndexOutOfBoundsException("index out of bounds");

    }

    public void setTupleList(int index, ArrayList<byte[]> tuples) {
        this.tupleCount.addAndGet(tuples.size());
        if (index < this.tuples.size())
            this.tuples.set(index, tuples);
        else if (index == this.tuples.size()) {
            this.tuples.add(index, tuples);
            for (int i = 0; i < tuples.size(); ++i) {
                addBytesCount(tuples.get(i).length);
            }
        } else
            throw new ArrayIndexOutOfBoundsException("index out of bounds");
    }

    public void setOffsetList(int index, ArrayList<Integer> offsets) {
        if (index < this.offsets.size())
            this.offsets.set(index, offsets);
        else if (index == this.offsets.size()) {
            this.offsets.add(index, offsets);
            addBytesCount(offsets.size() * (Integer.SIZE / Byte.SIZE));
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

    public void insertKeyValueList(TKey key, ArrayList<TValue> values) throws UnsupportedGenericException {

        int index = searchIndex(key);
        if (index < this.keys.size() && this.getKey(index).compareTo(key) == 0) {
            this.values.get(index).addAll(values);
        } else {
            this.keys.add(index, key);
            this.values.add(index, new ArrayList<TValue>(values));
            ++this.keyCount;
        }

    }

    public BTreeNode insertKeyValue(TKey key, byte[] serilizedTuple, boolean templateMode) throws UnsupportedGenericException{
        BTreeNode node = null;

        int index = searchIndex(key);

        if (!(index < this.keys.size() && this.getKey(index).compareTo(key) == 0)) {
            this.keys.add(index, key);
            addBytesCount(UtilGenerics.sizeOf(key.getClass()));
            this.tuples.add(index, new ArrayList<byte[]>());
            this.offsets.add(index, new ArrayList<Integer>());
            ++this.keyCount;
        }

        tupleCount.incrementAndGet();
        this.tuples.get(index).add(serilizedTuple);
        this.offsets.get(index).add(serilizedTuple.length);
        addBytesCount(serilizedTuple.length);
        addBytesCount(Integer.SIZE / Byte.SIZE);

        if (!templateMode && isOverflow()) {
            node = dealOverflow();
        }

        return node;
    }

    public BTreeNode insertKeyValue(TKey key, TValue value) throws UnsupportedGenericException{
        BTreeNode node = null;

        int index = searchIndex(key);
        if (index < this.keys.size() && this.getKey(index).compareTo(key) == 0) {
            this.values.get(index).add(value);
        } else {
            this.keys.add(index, key);
            this.values.add(index, new ArrayList<TValue>());
            this.values.get(index).add(value);
            ++this.keyCount;
        }

        if (isOverflow()) {
            node = dealOverflow();
        }

        return node;
    }

    public void insertKeyValueInBulkLoading(TKey key, TValue value) throws UnsupportedGenericException{
        keys.add(key);
        values.add(new ArrayList<TValue>(Arrays.asList(value)));
        ++keyCount;
    }


    /**
     * When splits a leaf node, the middle key is kept on new node and be pushed to parent node.
     */
    @Override
    protected BTreeNode<TKey> split() {

        BTreeLeafNode<TKey, TValue> newRNode = new BTreeLeafNode<TKey, TValue>(this.ORDER, counter);

        int midIndex = this.getKeyCount() / 2;

        for (int i = midIndex; i < this.getKeyCount(); ++i) {
            try {
                newRNode.setKey(i - midIndex, this.getKey(i));

                newRNode.addBytesCount(UtilGenerics.sizeOf(this.getKey(i).getClass()));

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

    /* The codes below are used to support deletion operation */
    public boolean delete(TKey key) {
        int index = this.search(key);
        if (index == -1)
            return false;
        this.deleteAt(index);
        return true;
    }

    private void deleteAt(int index) {
        try {
            substactBytesCount(UtilGenerics.sizeOf(this.keys.get(index).getClass()));
        } catch (UnsupportedGenericException e) {
            e.printStackTrace();
        }
        this.keys.remove(index);

        for (int i = 0; i < tuples.get(index).size(); ++i) {
            substactBytesCount(tuples.get(index).get(i).length);
        }

        this.tuples.remove(index);

        substactBytesCount(this.offsets.get(index).size() * (Integer.SIZE / Byte.SIZE));
        this.offsets.remove(index);
        --this.keyCount;
    }

    @Override
    protected void processChildrenTransfer(BTreeNode<TKey> borrower, BTreeNode<TKey> lender, int borrowIndex) {
        throw new UnsupportedOperationException();
    }

    @Override
    protected BTreeNode<TKey> processChildrenFusion(BTreeNode<TKey> leftChild, BTreeNode<TKey> rightChild) {
        throw new UnsupportedOperationException();
    }

    /**
     * Notice that the key sunk from parent is be abandoned.
     */
    @Override
    @SuppressWarnings("unchecked")
    protected void fusionWithSibling(TKey sinkKey, BTreeNode<TKey> rightSibling) {
        BTreeLeafNode<TKey, TValue> siblingLeaf = (BTreeLeafNode<TKey, TValue>) rightSibling;

        int j = this.getKeyCount();
        for (int i = 0; i < siblingLeaf.getKeyCount(); ++i) {
            try {
                this.setKey(j + i, siblingLeaf.getKey(i));
            } catch (UnsupportedGenericException e) {
                e.printStackTrace();
            }
            this.setValueList(j + i, siblingLeaf.getValueList(i));
        }
        this.keyCount += siblingLeaf.getKeyCount();

        this.setRightSibling(siblingLeaf.rightSibling);
        if (siblingLeaf.rightSibling != null)
            siblingLeaf.rightSibling.setLeftSibling(this);
    }

    @Override
    @SuppressWarnings("unchecked")
    protected TKey transferFromSibling(TKey sinkKey, BTreeNode<TKey> sibling, int borrowIndex) {
        BTreeLeafNode<TKey,TValue> siblingNode = (BTreeLeafNode<TKey,TValue>)sibling;
        try {
            this.insertKeyValueList(siblingNode.getKey(borrowIndex), siblingNode.getValueList(borrowIndex));
        } catch (UnsupportedGenericException e) {
            e.printStackTrace();
        }
        siblingNode.deleteAt(borrowIndex);

        return borrowIndex == 0 ? sibling.getKey(0) : this.getKey(0);
    }



    protected void clearNode() {
        for (TKey k : this.keys) {
            try {
                bytesCount -= UtilGenerics.sizeOf(k.getClass());
            } catch (UnsupportedGenericException e) {
                e.printStackTrace();
            }
        }

        for (int i = 0; i < tuples.size(); ++i) {
            for (int j = 0; j < tuples.get(i).size(); ++j) {
                bytesCount -= tuples.get(i).get(j).length;
            }
        }

        for (int i = 0; i < offsets.size(); ++i) {
            bytesCount -= offsets.get(i).size() * (Integer.SIZE / Byte.SIZE);
        }

        this.keys.clear();

        this.values.clear();

        this.tuples.clear();

        this.offsets.clear();

        this.keyCount = 0;

        tupleCount.set(0);
    }

    @Override
    public BTreeNode deepCopy(List<BTreeNode> nodes) {
        BTreeLeafNode node = new BTreeLeafNode(ORDER, counter.clone());
        node.keyCount = keyCount;

        node.keys = (ArrayList) keys.clone();

        node.tuples = (ArrayList) tuples.clone();

        node.offsets = (ArrayList) offsets.clone();

        node.tupleCount.set(this.tupleCount.get());

        node.bytesCount = bytesCount;

        nodes.add(node);
        return node;
    }


    /* The code below is used to support search operation.*/
    public ArrayList<byte []> searchAndGetTuples(TKey key) {
        ArrayList<byte[]> tuples = new ArrayList<byte[]>();
        int index = search(key);
        tuples = (index == -1 ? new ArrayList<byte[]>() : getTuples(index));
        return tuples;
    }

    public ArrayList<byte []> searchAndGetTuplesInTemplate(TKey key) {
        ArrayList<byte[]> tuples;
        int index = search(key);
        tuples = (index == -1 ? new ArrayList<byte[]>() : getTuples(index));
        return tuples;
    }

    public List<byte[]> searchRange(TKey leftKey, TKey rightKey){
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
                }
                currLeaf = (BTreeLeafNode<TKey,TValue>) currLeaf.rightSibling;
            }

            currIndex = 0;
        }

        while (currLeaf != null && currLeaf.getKey(currIndex).compareTo(rightKey) <= 0) {
            assert currLeaf.lock.getReadLockCount() > 0;
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
                        }
                        currLeaf = (BTreeLeafNode<TKey,TValue>) currLeaf.rightSibling;
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

    public void addBytesCount(int len) {
        bytesCount += len;
    }

    public void substactBytesCount(int len) {
        bytesCount -= len;
    }

    public long getTupleCount() {
        return tupleCount.get();
    }

    public int getBytesCount() {
        return bytesCount;
    }

    public void setKeys(ArrayList<TKey> keys) {
        this.keys = keys;
    }

    public void setTuples(ArrayList<ArrayList<byte[]>> tuples) {
        this.tuples = tuples;
    }

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