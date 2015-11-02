package indexingTopology.util;

import indexingTopology.exception.UnsupportedGenericException;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

class BTreeLeafNode<TKey extends Comparable<TKey>> extends BTreeNode<TKey> {
	protected ArrayList<byte[]> values;
	
	public BTreeLeafNode(int order, BytesCounter counter) {
        super(order,counter);
        this.keys = new ArrayList<TKey>();
		this.values = new ArrayList<byte[]>();
	}

	@SuppressWarnings("unchecked")
	public byte[] getValue(int index) {
		return this.values.get(index);
	}

	public void setValue(int index, byte[] value) {
        if (index<this.values.size())
		    this.values.set(index,value);
        else if (index==this.values.size()) {
            this.counter.countValueAddition(value.length);
            this.values.add(index, value);
        }
        else
            throw new ArrayIndexOutOfBoundsException("index out of bounds");
	}
	
	@Override
	public TreeNodeType getNodeType() {
		return TreeNodeType.LeafNode;
	}
	
	@Override
    // TODO optimize to binary search
	public int search(TKey key) {
		for (int i = 0; i < this.getKeyCount(); ++i) {
			 int cmp = this.getKey(i).compareTo(key);
			 if (cmp == 0) {
				 return i;
			 }
			 else if (cmp > 0) {
				 return -1;
			 }
		}
		
		return -1;
	}

    public Collection<BTreeNode<TKey>> recursiveSerialize(ByteBuffer allocatedBuffer) {
        allocatedBuffer.put((byte) 'l');
        allocatedBuffer.putInt(this.getKeyCount());
        for (int i=0;i<this.keys.size();i++) {
            try {
                UtilGenerics.putIntoByteBuffer(allocatedBuffer,this.keys.get(i));
            } catch (UnsupportedGenericException e) {
                e.printStackTrace();
            }

            allocatedBuffer.putInt(this.values.get(i).length);
            allocatedBuffer.put(this.values.get(i));
        }

        return null;
    }

	/* The codes below are used to support insertion operation */
	
	public void insertKey(TKey key, byte[] value) throws UnsupportedGenericException {
        counter.countKeyAddition(UtilGenerics.sizeOf(key.getClass()));
        counter.countValueAddition(value.length);

		int index = 0;
		while (index < this.getKeyCount() && this.getKey(index).compareTo(key) < 0)
			++index;

        this.keys.add(index,key);
        this.values.add(index,value);
        ++this.keyCount;

	}

	/**
	 * When splits a leaf node, the middle key is kept on new node and be pushed to parent node.
	 */
	@Override
	protected BTreeNode<TKey> split() {
		int midIndex = this.getKeyCount() / 2;
		
		BTreeLeafNode<TKey> newRNode = new BTreeLeafNode<TKey>(this.ORDER,counter);
		for (int i = midIndex; i < this.getKeyCount(); ++i) {
            try {
                newRNode.setKey(i - midIndex, this.getKey(i));
            } catch (UnsupportedGenericException e) {
                e.printStackTrace();
            }
            newRNode.setValue(i - midIndex, this.getValue(i));
		}

		newRNode.keyCount = this.getKeyCount() - midIndex;

        for (int i=this.getKeyCount()-1;i>=midIndex;i--)
            this.deleteAt(i);

        this.keyCount=midIndex;
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
            counter.countKeyRemoval(UtilGenerics.sizeOf(this.keys.get(index).getClass()));
        } catch (UnsupportedGenericException e) {
            e.printStackTrace();
        }

        counter.countValueRemoval(this.values.get(index).length);
        this.keys.remove(index);
        this.values.remove(index);
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
		BTreeLeafNode<TKey> siblingLeaf = (BTreeLeafNode<TKey>)rightSibling;
		
		int j = this.getKeyCount();
		for (int i = 0; i < siblingLeaf.getKeyCount(); ++i) {
            try {
                this.setKey(j + i, siblingLeaf.getKey(i));
            } catch (UnsupportedGenericException e) {
                e.printStackTrace();
            }
            this.setValue(j + i, siblingLeaf.getValue(i));
		}
		this.keyCount += siblingLeaf.getKeyCount();
		
		this.setRightSibling(siblingLeaf.rightSibling);
		if (siblingLeaf.rightSibling != null)
			siblingLeaf.rightSibling.setLeftSibling(this);
	}
	
	@Override
	@SuppressWarnings("unchecked")
	protected TKey transferFromSibling(TKey sinkKey, BTreeNode<TKey> sibling, int borrowIndex) {
		BTreeLeafNode<TKey> siblingNode = (BTreeLeafNode<TKey>)sibling;
        try {
            this.insertKey(siblingNode.getKey(borrowIndex), siblingNode.getValue(borrowIndex));
        } catch (UnsupportedGenericException e) {
            e.printStackTrace();
        }
        siblingNode.deleteAt(borrowIndex);
		
		return borrowIndex == 0 ? sibling.getKey(0) : this.getKey(0);
	}

    public List<byte[]> searchRange(TKey leftKey, TKey rightKey) {
        // find first index satisfying range
        int firstIndex;
        for (firstIndex=0;firstIndex<this.getKeyCount();firstIndex++) {
            int cmp = this.getKey(firstIndex).compareTo(leftKey);
            if (cmp >= 0)
                break;
        }

        List<byte[]> retList = new ArrayList<byte[]>();
        BTreeLeafNode<TKey> currLeaf=this;
        int currIndex=firstIndex;

        // case when all keys in the node are smaller than leftKey - shift to next rightSibling
        if (firstIndex>=this.getKeyCount()) {
            currLeaf = (BTreeLeafNode<TKey>) this.rightSibling;
            currIndex = 0;
        }

        while (currLeaf!=null && currLeaf.getKey(currIndex).compareTo(rightKey)<=0) {
            retList.add(currLeaf.getValue(currIndex));
            currIndex++;
            if (currIndex>=currLeaf.getKeyCount()) {
                currLeaf = (BTreeLeafNode<TKey>) currLeaf.rightSibling;
                currIndex = 0;
            }
        }

        return retList;
    }

	protected void clearNode() {
		// counter updates
		for (TKey k : this.keys) {
			try {
				counter.countKeyRemoval(UtilGenerics.sizeOf(k.getClass()));
			} catch (UnsupportedGenericException e) {
				e.printStackTrace();
			}
		}

		for (byte[] val : this.values) {
			counter.countValueRemoval(val.length);
		}

		// clear node
		this.keys.clear();
		this.values.clear();
		this.keyCount=0;
	}
}
