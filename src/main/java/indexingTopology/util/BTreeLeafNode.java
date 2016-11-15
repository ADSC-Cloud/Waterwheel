package indexingTopology.util;

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

	public BTreeLeafNode(int order, BytesCounter counter) {
		super(order,counter);
		this.keys = new ArrayList<TKey>(order);
		this.values = new ArrayList<ArrayList<TValue>>(order + 1);
		this.tuples = new ArrayList<ArrayList<byte []>>(order + 1);
		this.offsets = new ArrayList<ArrayList<Integer>>(order + 1);
		bytesCount = 0;
	}

	public boolean validateParentReference() {
		return true;
	}

    public boolean validateNoDuplicatedChildReference() {
        return true;
    }

	public boolean validateAllLockReleased() {
//		if(lock.writeLock(). || lock.getReadLockCount() > 0) {
//			return false;
//		} else {
//			return true;
//		}
		return true;
	}

	public int getDepth() {
		return 1;
	}

	public BTreeLeafNode(BTreeNode oldNode) throws CloneNotSupportedException{
		super(oldNode.ORDER, (BytesCounter) oldNode.counter.clone());
		this.keys = new ArrayList<TKey>();
		this.keys.addAll(oldNode.keys);

	/*	for (TKey key : keys) {
			System.out.print(key);
		}*/
		//	System.out.println();
	}

	@SuppressWarnings("unchecked")
	public ArrayList<TValue> getValueList(int index) {
		ArrayList<TValue> values;
        values = this.values.get(index);
		return values;
	}

	public ArrayList<byte[]> getTuples(int index) {
		ArrayList<byte[]> tuples;
		tuples = this.tuples.get(index);
		return tuples;
	}

	public ArrayList<Integer> getOffsets(int index) {
		ArrayList<Integer> offsets;
		offsets = this.offsets.get(index);
		return offsets;
	}


	public void setValueList(int index, ArrayList<TValue> value) {

//		try {
			if (index < this.values.size())
				this.values.set(index, value);
			else if (index == this.values.size()) {
				// TODO fix this
//            this.counter.countValueAddition(value.length);
				this.values.add(index, value);
			} else
				throw new ArrayIndexOutOfBoundsException("index out of bounds");
//		} finally {
//			releaseWriteLock();
//		}

	}


	public void setTupleList(int index, ArrayList<byte[]> tuples) {
		if (index < this.tuples.size())
			this.tuples.set(index, tuples);
		else if (index == this.tuples.size()) {
			// TODO fix this
			this.tuples.add(index, tuples);
			for (int i = 0; i < tuples.size(); ++i) {
//				bytesCount += tuples.get(i).length;
				addBytesCount(tuples.get(i).length);
			}
		} else
			throw new ArrayIndexOutOfBoundsException("index out of bounds");
	}

	public void setOffsetList(int index, ArrayList<Integer> offsets) {
		if (index < this.offsets.size())
			this.offsets.set(index, offsets);
		else if (index == this.offsets.size()) {
			// TODO fix this
			this.offsets.add(index, offsets);
//			bytesCount += (offsets.size() * (Integer.SIZE / Byte.SIZE));
			addBytesCount(offsets.size() * (Integer.SIZE / Byte.SIZE));
		} else
			throw new ArrayIndexOutOfBoundsException("index out of bounds");
	}



	@Override
	public TreeNodeType getNodeType() {
		return TreeNodeType.LeafNode;
	}

//	@Override
//	 TODO optimize to binary search
//	public int search(TKey key) {
//		for (int i = 0; i < this.getKeyCount(); ++i) {
//			 int cmp = this.getKey(i).compareTo(key);
//			 if (cmp == 0) {
//				 return i;
//			 }
//			 else if (cmp > 0) {
//				 return -1;
//			 }
//		}
//		return -1;
//	}

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



	public Collection<BTreeNode<TKey>> recursiveSerialize(ByteBuffer allocatedBuffer) {
		// TODO fix this
//        allocatedBuffer.put((byte) 'l');
//        allocatedBuffer.putInt(this.getKeyCount());
//        for (int i=0;i<this.keys.size();i++) {
//            try {
//                UtilGenerics.putIntoByteBuffer(allocatedBuffer,this.keys.get(i));
//            } catch (UnsupportedGenericException e) {
//                e.printStackTrace();
//            }
//
//            allocatedBuffer.putInt(this.values.get(i).length);
//            allocatedBuffer.put(this.values.get(i));
//        }
//
		return null;
	}

	/**
	 * The content of the byte array is as following
	 * [key count of the leave  [key1, key2 ...] [number of tuples of each key] [offset of each tuple] [content of each
	 * tuple] [number of tuples of each key] [offset of each tuple] [content of each
	 * tuple] ....]
	 * @return the serialized leaf in a byte array
	 */



	public byte[] serialize() {
//		System.out.println("The size of keys is " + getKeyCount());
//		System.out.println("The size of tuples are " + tuples.size());
//		System.out.println("The size of offsets are " + offsets.size());
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
		int totalBytes = bytesCount + (1 + this.tuples.size()) * (Integer.SIZE / Byte.SIZE);
//		System.out.println("Total byttes " + totalBytes);
		byte[] b = ByteBuffer.allocate(Integer.SIZE / Byte.SIZE).putInt(totalBytes).array();
		writeToByteArrayOutputStream(bos, b);
//        byte[] b = ByteBuffer.allocate(Integer.SIZE / Byte.SIZE).putInt(this.keys.size()).array();
        b = ByteBuffer.allocate(Integer.SIZE / Byte.SIZE).putInt(this.keys.size()).array();
        writeToByteArrayOutputStream(bos, b);
        for (TKey key : keys) {
            b = ByteBuffer.allocate(Double.SIZE / Byte.SIZE).putDouble((Double) key).array();
            writeToByteArrayOutputStream(bos, b);
        }
//        b = ByteBuffer.allocate(Integer.SIZE / Byte.SIZE).putInt(this.tuples.size()).array();
//        writeToByteArrayOutputStream(bos, b);
        for (int i = 0;i < this.keys.size(); i++) {
//			System.out.println("Key count " + this.keys.size());
//			System.out.println("Number of tuple of corresponding key " + this.keys.get(i) + " " + this.tuples.get(i).size());
			b = ByteBuffer.allocate(Integer.SIZE / Byte.SIZE).putInt(this.tuples.get(i).size()).array();
			writeToByteArrayOutputStream(bos, b);
            for (int j = 0; j < this.tuples.get(i).size(); ++j) {
				b = ByteBuffer.allocate(Integer.SIZE / Byte.SIZE).putInt(this.offsets.get(i).get(j)).array();
				writeToByteArrayOutputStream(bos, b);
                writeToByteArrayOutputStream(bos, this.tuples.get(i).get(j));
            }
        }
        return bos.toByteArray();
    }

	public BTreeLeafNode deserialize(byte [] b, int BTreeOrder, BytesCounter counter) throws IOException {
		BTreeLeafNode leaf = new BTreeLeafNode(BTreeOrder, counter);
		int len = Integer.SIZE / Byte.SIZE;
		int offset = 0;
		int keyCount = ByteBuffer.wrap(b, offset, len).getInt();
		offset += len;
		ArrayList<Double> keys = new ArrayList<Double>();
		for (int i = 0; i < keyCount;i++) {
			len = Double.SIZE / Byte.SIZE;
			Double key = ByteBuffer.wrap(b, offset, len).getDouble();
			keys.add(key);
			offset += len;
		}
		leaf.keys = keys;
		ArrayList<ArrayList<byte[]>> tuples = new ArrayList<ArrayList<byte[]>>();
		for (int i = 0; i < keys.size();i++) {
			len = Integer.SIZE / Byte.SIZE;
			int tupleCount = ByteBuffer.wrap(b, offset, len).getInt();
			tuples.add(new ArrayList<byte[]>());
			offset += len;
			for (int j = 0; j < tupleCount; ++j) {
				int lengthOfTuple = ByteBuffer.wrap(b, offset, len).getInt();
				offset += len;
				byte[] tuple = new byte[lengthOfTuple];
				ByteBuffer.wrap(b, offset, lengthOfTuple).get(tuple);
				tuples.get(i).add(tuple);
				offset += lengthOfTuple;
			}
		}
		leaf.tuples = tuples;
		return leaf;
	}




	private void writeToByteArrayOutputStream(ByteArrayOutputStream bos, byte[] b) {
        try {
            bos.write(b);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

	/* The codes below are used to support insertion operation */

/*	public void insertKeyValue(TKey key, TValue value, TimingModule tm) throws UnsupportedGenericException {
//		int index = 0;
//		while (index < this.getKeyCount() && this.getKey(index).compareTo(key) < 0)
//			++index;
//		tm.startTiming(Constants.TIME_SEARCH_INDEX.str);
//		int index = searchIndex(key);
//		tm.endTiming(Constants.TIME_SEARCH_INDEX.str);
//
//		tm.startTiming(Constants.TIME_INSERT_INTO_ARRAYLIST.str);
//		if (index<this.keys.size() && this.getKey(index).compareTo(key)==0) {
//			this.values.get(index).add(value);
//		} else {
//			this.keys.add(index, key);
//			this.values.add(index, new ArrayList<TValue>(Arrays.asList(value)));
//			++this.keyCount;
//		}
		keys.add(key);
		values.add(new ArrayList<TValue>(Arrays.asList(value)));
		++keyCount;
//		tm.endTiming(Constants.TIME_INSERT_INTO_ARRAYLIST.str);
	}*/

//	public void insertKeyValue(TKey key, TValue value, boolean templateMode) throws UnsupportedGenericException {
//		int index = 0;
//		while (index < this.getKeyCount() && this.getKey(index).compareTo(key) < 0)
//			++index;
//		tm.startTiming(Constants.TIME_SEARCH_INDEX.str);
//		int index = searchIndex(key);
//		tm.endTiming(Constants.TIME_SEARCH_INDEX.str);
//
//		tm.startTiming(Constants.TIME_INSERT_INTO_ARRAYLIST.str);
//		if (index<this.keys.size() && this.getKey(index).compareTo(key)==0) {
//			this.values.get(index).add(value);
//		} else {
//			this.keys.add(index, key);
//			this.values.add(index, new ArrayList<TValue>(Arrays.asList(value)));
//			++this.keyCount;
//		}
//		wLock.lock();
//		try {
//			keys.add(key);
//			values.add(new ArrayList<TValue>(Arrays.asList(value)));
//			++keyCount;
//		} finally {
//			wLock.unlock();
//		}
//	}


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
//		int index = 0;
//		while (index < this.getKeyCount() && this.getKey(index).compareTo(key) < 0)
//	  		++index;
//		acquireWriteLock();
//		try {
//			int index = searchIndex(key);

			int index = searchIndex(key);
			if (index < this.keys.size() && this.getKey(index).compareTo(key) == 0) {
				this.values.get(index).addAll(values);
			} else {
				this.keys.add(index, key);
				this.values.add(index, new ArrayList<TValue>(values));
				++this.keyCount;
			}
//		} finally {
//			releaseWriteLock();
//		}

	}

	/**
	 * When splits a leaf node, the middle key is kept on new node and be pushed to parent node.
	 */
	@Override
	protected BTreeNode<TKey> split() {
//		acquireWriteLock();
//		System.out.println(String.format("Leaf node %d is split!", getId()));
		BTreeLeafNode<TKey, TValue> newRNode = new BTreeLeafNode<TKey, TValue>(this.ORDER, counter);
//        try {
//			Collections.sort(keys);

			int midIndex = this.getKeyCount() / 2;

			for (int i = midIndex; i < this.getKeyCount(); ++i) {
				try {
					newRNode.setKey(i - midIndex, this.getKey(i));
//					newRNode.bytesCount += UtilGenerics.sizeOf(this.getKey(i).getClass());
					newRNode.addBytesCount(UtilGenerics.sizeOf(this.getKey(i).getClass()));
				} catch (UnsupportedGenericException e) {
					e.printStackTrace();
				}
//				newRNode.setValueList(i - midIndex, this.getValueList(i));
				newRNode.setTupleList(i - midIndex, this.getTuples(i));
				newRNode.setOffsetList(i - midIndex, this.getOffsets(i));
			}

			newRNode.keyCount = this.getKeyCount() - midIndex;

			for (int i = this.getKeyCount() - 1; i >= midIndex; i--)
				this.deleteAt(i);
			this.keyCount = midIndex;
//		} finally {
//			releaseWriteLock();
//		}
		return newRNode;
	}


	@Override
	protected BTreeNode<TKey> pushUpKey(TKey key, BTreeNode<TKey> leftChild, BTreeNode<TKey> rightNode) {
		throw new UnsupportedOperationException();
	}




	/* The codes below are used to support deletion operation */
	public boolean delete(TKey key) {
//		acquireWriteLock();
//		try {
			int index = this.search(key);
			if (index == -1)
				return false;
//		System.out.println(index);
			this.deleteAt(index);
			return true;
//		} finally {
//			releaseWriteLock();
//		}

	}

	public void deleteKeyValue(TKey key, TValue value) throws UnsupportedGenericException {
//		acquireWriteLock();
//		try {
			int index = 0;
			while (index < this.getKeyCount() && this.getKey(index).compareTo(key) < 0)
				++index;

			if (index < this.keys.size() && this.getKey(index).compareTo(key) == 0) {
				int indexLast = this.values.get(index).lastIndexOf(value);
				this.values.get(index).remove(indexLast);
			}
//		} finally {
//			releaseWriteLock();
//		}

	}

	private void deleteAt(int index) {
//		acquireWriteLock();
//		try {
//			try {
//				counter.countKeyRemoval(UtilGenerics.sizeOf(this.keys.get(index).getClass()));
//			} catch (UnsupportedGenericException e) {
//				e.printStackTrace();
//			}

			// TODO fix this
//        counter.countValueRemoval(this.values.get(index).length);
		try {
//			bytesCount -= UtilGenerics.sizeOf(this.keys.get(index).getClass());
			substactBytesCount(UtilGenerics.sizeOf(this.keys.get(index).getClass()));
		} catch (UnsupportedGenericException e) {
			e.printStackTrace();
		}
		    this.keys.remove(index);
//			this.values.remove(index);
		    for (int i = 0; i < tuples.get(index).size(); ++i) {
//				bytesCount -= tuples.get(index).get(i).length;
				substactBytesCount(tuples.get(index).get(i).length);
			}
		    this.tuples.remove(index); //Added for tuples
//		    bytesCount -= this.offsets.get(index).size() * (Integer.SIZE / Byte.SIZE);
		substactBytesCount(this.offsets.get(index).size() * (Integer.SIZE / Byte.SIZE));
		    this.offsets.remove(index); //Added for tuples
			--this.keyCount;
//		} finally {
//			releaseWriteLock();
//		}
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
//		acquireWriteLock();
//		try {
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
//		} finally {
//			releaseWriteLock();
//		}

	}

	@Override
	@SuppressWarnings("unchecked")
	protected TKey transferFromSibling(TKey sinkKey, BTreeNode<TKey> sibling, int borrowIndex) {
//		acquireWriteLock();
		BTreeLeafNode<TKey,TValue> siblingNode = (BTreeLeafNode<TKey,TValue>)sibling;
//		try {
			try {
				this.insertKeyValueList(siblingNode.getKey(borrowIndex), siblingNode.getValueList(borrowIndex));
			} catch (UnsupportedGenericException e) {
				e.printStackTrace();
			}
			siblingNode.deleteAt(borrowIndex);
//		} finally {
//			releaseWriteLock();
//		}

		return borrowIndex == 0 ? sibling.getKey(0) : this.getKey(0);
	}
/*
	public List<TValue> searchRange(TKey leftKey, TKey rightKey) {
		// find first index satisfying range
//		rLock.lock();
//        System.out.println("The number of keys is " + this.getKeyCount());
		List<TValue> retList = new ArrayList<TValue>();
		BTreeLeafNode<TKey, TValue> currLeaf = this;
//        System.out.println("The number of keys is " + this.getKeyCount());
//		try {
			int firstIndex;
			for (firstIndex = 0; firstIndex < this.getKeyCount(); ++firstIndex) {
				int cmp = this.getKey(firstIndex).compareTo(leftKey);
				if (cmp >= 0)
					break;
			}

//			List<TValue> retList = new ArrayList<TValue>();
//			BTreeLeafNode<TKey, TValue> currLeaf = this;
			int currIndex = firstIndex;

			// case when all keys in the node are smaller than leftKey - shift to next rightSibling
			if (firstIndex >= this.getKeyCount()) {
//                System.out.println("The number of keys is " + this.getKeyCount());
				if (this.rightSibling != null) {
					this.rightSibling.acquireReadLock();
                    releaseReadLock();
					currLeaf = (BTreeLeafNode<TKey, TValue>) getRightSibling();
					currIndex = 0;
				}
			}
            System.out.println(currIndex);
            System.out.println(currLeaf == this);
			while (currLeaf != null && currLeaf.getKey(currIndex).compareTo(rightKey) <= 0) {
                currLeaf.print();
				retList.addAll(currLeaf.getValueList(currIndex));
				currIndex++;
				if (currIndex >= currLeaf.getKeyCount()) {
					if (currLeaf.rightSibling != null) {
						currLeaf.rightSibling.acquireReadLock();
                        currLeaf.releaseReadLock();
						currLeaf = (BTreeLeafNode<TKey, TValue>) currLeaf.getRightSibling();
						currIndex = 0;
					}
				}
			}
//		} finally {
//			rLock.unlock();
			if (currLeaf != null && currLeaf != this) {
				currLeaf.releaseReadLock();
			}
//		}
		return retList;
	} */

    public List<byte[]> searchRange(TKey leftKey, TKey rightKey){
        // find first index satisfying range
		int firstIndex;
		Lock lastLock = this.getrLock();
		for (firstIndex = 0; firstIndex < this.getKeyCount(); firstIndex++) {
			int cmp = this.getKey(firstIndex).compareTo(leftKey);
			if (cmp >= 0)
				break;
		}
//		System.out.println("The most left leave is ");
//		this.print();
//        List<BTreeNode> nodes = new ArrayList<BTreeNode>();
		List<byte[]> retList = new ArrayList<byte[]>();
		BTreeLeafNode<TKey,TValue> currLeaf = this;
		BTreeNode<TKey> currRightSibling = this;
		BTreeNode<TKey> tmpNode = this;
//        nodes.add(this);
		int currIndex = firstIndex;

		assert currLeaf.lock.getReadLockCount() > 0;

		// case when all keys in the node are smaller than leftKey - shift to next rightSibling
		if (firstIndex >= this.getKeyCount()) {
			currLeaf = (BTreeLeafNode<TKey,TValue>) this.rightSibling;
//            nodes.add(currLeaf);
			if (currLeaf != null) {
				tmpNode = this;
				currRightSibling = this.rightSibling;
				currLeaf.acquireReadLock();
				if (lastLock != null) {
					lastLock.unlock();
				}
				lastLock = currLeaf.getrLock();
			}
//			releaseReadLock();
//			if (lastLock != null) {
//				lastLock.unlock();
//			}
			currIndex = 0;
		}
		while (currLeaf != null && currLeaf.getKey(currIndex).compareTo(rightKey) <= 0) {
//			if (currRightSibling != currLeaf) {
//				System.out.println(false);
//			}
//			assert currLeaf != null;
//			if (currLeaf.lock.getReadLockCount() == 0) {
//				System.out.println(currIndex);
//				currLeaf.leftSibling.print();
//				currLeaf.print();
//				currLeaf.rightSibling.print();
//			}
//			currLeaf.print();
			assert currLeaf.lock.getReadLockCount() > 0;
//			assert (currRightSibling == currLeaf || currRightSibling == this);
			retList.addAll(currLeaf.getTuples(currIndex));
			currIndex++;
			if (currIndex >= currLeaf.getKeyCount()) {

				if (currLeaf.rightSibling != null) {
					tmpNode = currLeaf;


//					System.out.println("Tmp Node is ");


//					tmpNode.print();
					currRightSibling = currLeaf.rightSibling;


//					System.out.println("Right sibling is");


					currRightSibling.acquireReadLock();
					if (lastLock != null) {
//						System.out.println(currIndex);
//						System.out.println(this.writeLockThreadId);
						lastLock.unlock();
					}
					lastLock = currRightSibling.getrLock();
//					currLeaf.rightSibling.print();


//					System.out.println("Current leaf is");


//					currLeaf.print();
//                    nodes.add((currLeaf.rightSibling));
					currLeaf = (BTreeLeafNode<TKey,TValue>) currLeaf.rightSibling;
					while (currRightSibling != currLeaf) {
						currRightSibling = tmpNode.rightSibling;


//						System.out.println("***** Right sibling is");
//						currRightSibling.print();

						lastLock.unlock();
						currLeaf = (BTreeLeafNode) tmpNode.rightSibling;

//						System.out.println("***** Current leaf is");
//						currLeaf.print();

						currLeaf.acquireReadLock();
						lastLock = currLeaf.getrLock();
					}
				} else {
//				currLeaf.releaseReadLock();
					currLeaf = (BTreeLeafNode<TKey, TValue>) currLeaf.rightSibling;
				}
				if (currLeaf != null) {
//					System.out.println("Current leaf is ");
//					currLeaf.print();
					assert currLeaf.lock.getReadLockCount() > 0;
				}
				currIndex = 0;
			}
		}
//		if (currLeaf != null) {
//			currLeaf.releaseReadLock();
//		} else if (currLeaf == this) {
//			currLeaf.releaseReadLock();
//		}
		if (lastLock != null) {
			lastLock.unlock();
		}
//        for (BTreeNode node : nodes) {
//            System.out.println(node.lock.getReadLockCount());
//            assert node.lock.getReadLockCount() == 0;
//        }
//        currLeaf.print();
//        System.out.println("The number of lock is " + this.lock.getReadLockCount());
//        assert this.lock.getReadLockCount() == 0;
		return retList;
    }

	protected void clearNode() {
		// counter updates
//        wLock.lock();
//		try {
			for (TKey k : this.keys) {
				try {
					bytesCount -= UtilGenerics.sizeOf(k.getClass());
				} catch (UnsupportedGenericException e) {
					e.printStackTrace();
				}
			}

			for (int i = 0; i < tuples.size(); ++i) {
				for (int j = 0; j < tuples.get(i).size(); ++j)
				    bytesCount -= tuples.get(i).get(j).length;
			}

		    for (int i = 0; i < offsets.size(); ++i) {
				bytesCount -= offsets.get(i).size() * (Integer.SIZE / Byte.SIZE);
		    }


			// Todo fix this
//		for (byte[] val : this.values) {
//			counter.countValueRemoval(val.length);
//		}

			// clear node
			this.keys.clear();
			this.values.clear();
		    this.tuples.clear();
		    this.offsets.clear();
			this.keyCount = 0;
//		System.out.println(bytesCount);
//		} finally {
//			wLock.unlock();
//		}
	}



	public Object clone(BTreeNode oldNode) throws CloneNotSupportedException{
		BTreeLeafNode node = new BTreeLeafNode(ORDER, (BytesCounter) counter.clone());
		node.keyCount = keyCount;
		if (parentNode != null) {
			node.parentNode = oldNode;
		}
		if (leftSibling != null) {
			node.leftSibling = new BTreeLeafNode(leftSibling);
		}
		if (rightSibling != null) {
			node.rightSibling = new BTreeLeafNode(rightSibling);
		}

		node.keys.addAll(keys);

		return node;
	}


	public static Object deepClone(Object object) {
		try {
			ByteArrayOutputStream baos = new ByteArrayOutputStream();
			ObjectOutputStream oos = new ObjectOutputStream(baos);
			oos.writeObject(object);
			ByteArrayInputStream bais = new ByteArrayInputStream(baos.toByteArray());
			ObjectInputStream ois = new ObjectInputStream(bais);
			return ois.readObject();
		}
		catch (Exception e) {
			e.printStackTrace();
			return null;
		}
	}

	public ArrayList<TValue> searchAndGetValues(TKey key) {
//		acquireReadLock();
		ArrayList<TValue> values;
//		try {
			int index = search(key);
			values = (index == -1 ? null : getValueList(index));
//		} finally {
//			releaseReadLock();
//		}
		return values;
	}

	public ArrayList<byte []> searchAndGetTuples(TKey key) {
		ArrayList<byte[]> tuples;
		int index = search(key);
		tuples = (index == -1 ? null : getTuples(index));
//            releaseReadLock();  //Added to check the paper
//        }
		return tuples;
	}


	public ArrayList<TValue> searchAndGetValuesInTemplate(TKey key) {

//		acquireReadLock();
//		ArrayList<TValue> values;
//		try {
//			int index = search(key);
//			values = (index == -1 ? null : getValueList(index));
//		} finally {
//			releaseReadLock();  //Added to check the paper
//		}
//		releaseWriteLock();
		ArrayList<TValue> values;
        int index = search(key);
        values = (index == -1 ? null : getValueList(index));
//            releaseReadLock();  //Added to check the paper
//        }
		return values;
	}

	public ArrayList<byte []> searchAndGetTuplesInTemplate(TKey key) {
		ArrayList<byte[]> tuples;
		int index = search(key);
		tuples = (index == -1 ? null : getTuples(index));
//            releaseReadLock();  //Added to check the paper
//        }
		return tuples;
	}

	public void insertKeyValueInTemplateMode(TKey key, TValue value) throws UnsupportedGenericException{
		acquireWriteLock();
		try {
//			keys.add(key);
//			values.add(new ArrayList<TValue>(Arrays.asList(value)));
//			++keyCount;

			int index = searchIndex(key);
			if (index < this.keys.size() && this.getKey(index).compareTo(key) == 0) {
				this.values.get(index).add(value);
			} else {
				this.keys.add(index, key);
				this.values.add(index, new ArrayList<TValue>());
				this.values.get(index).add(value);
				++this.keyCount;
			}

		} finally {
			releaseWriteLock();
		}
	}

	public void insertKeyValueInTemplateMode(TKey key, byte[] serilizedTuple) throws UnsupportedGenericException{
		acquireWriteLock();
		try {
//			keys.add(key);
//			values.add(new ArrayList<TValue>(Arrays.asList(value)));
//			++keyCount;

			int index = searchIndex(key);
			if (index < this.keys.size() && this.getKey(index).compareTo(key) == 0) {
				this.tuples.get(index).add(serilizedTuple);
//				bytesCount += serilizedTuple.length;
				addBytesCount(serilizedTuple.length);
				this.offsets.get(index).add(serilizedTuple.length);
//				bytesCount += Integer.SIZE / Byte.SIZE;
				addBytesCount(Integer.SIZE / Byte.SIZE);
			} else {
				this.keys.add(index, key);
//				bytesCount += UtilGenerics.sizeOf(key.getClass());
				addBytesCount(UtilGenerics.sizeOf(key.getClass()));
				this.tuples.add(index, new ArrayList<byte[]>());
				this.tuples.get(index).add(serilizedTuple);
//				bytesCount += serilizedTuple.length;
				addBytesCount(serilizedTuple.length);
				this.offsets.add(index, new ArrayList<Integer>());
				this.offsets.get(index).add(serilizedTuple.length);
//				bytesCount += Integer.SIZE / Byte.SIZE;
				addBytesCount(Integer.SIZE / Byte.SIZE);
				++this.keyCount;
			}

		} finally {
			releaseWriteLock();
		}
	}




	public BTreeNode insertKeyValue(TKey key, byte[] serilizedTuple) throws UnsupportedGenericException{
//		acquireWriteLock();

//		BTreeNode node = null;
//		try {
//			keys.add(key);
//			values.add(new ArrayList<TValue>(Arrays.asList(value)));
//			++keyCount;

//		checkIfCurrentHoldAnyLock();
		BTreeNode node = null;
//		try {
//			keys.add(key);
//			values.add(new ArrayList<TValue>(Arrays.asList(value)));
//			++keyCount;

			int index = searchIndex(key);
			if (index < this.keys.size() && this.getKey(index).compareTo(key) == 0) {
				this.tuples.get(index).add(serilizedTuple);
//				bytesCount += serilizedTuple.length;
				addBytesCount(serilizedTuple.length);
				this.offsets.get(index).add(serilizedTuple.length);
//				bytesCount += Integer.SIZE / Byte.SIZE;
				addBytesCount(Integer.SIZE / Byte.SIZE);
			} else {
				this.keys.add(index, key);
//				bytesCount += UtilGenerics.sizeOf(key.getClass());
				addBytesCount(UtilGenerics.sizeOf(key.getClass()));
				this.tuples.add(index, new ArrayList<byte[]>());
				this.tuples.get(index).add(serilizedTuple);
//				bytesCount += serilizedTuple.length;
				addBytesCount(serilizedTuple.length);
				this.offsets.add(index, new ArrayList<Integer>());
				this.offsets.get(index).add(serilizedTuple.length);
//				bytesCount += Integer.SIZE / Byte.SIZE;
				addBytesCount(Integer.SIZE / Byte.SIZE);
				++this.keyCount;
			}


			if (isOverflow()) {
				node = dealOverflow();
			}
//		} finally {
//			releaseWriteLock();
//		}
		return node;
	}

	public BTreeNode insertKeyValue(TKey key, TValue value) throws UnsupportedGenericException{
//		acquireWriteLock();

//		BTreeNode node = null;
//		try {
//			keys.add(key);
//			values.add(new ArrayList<TValue>(Arrays.asList(value)));
//			++keyCount;

//		checkIfCurrentHoldAnyLock();
		BTreeNode node = null;
//		try {
//			keys.add(key);
//			values.add(new ArrayList<TValue>(Arrays.asList(value)));
//			++keyCount;

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
//		} finally {
//			releaseWriteLock();
//		}
		return node;
	}




	public void insertKeyValueWithoutOverflow(TKey key, TValue value) throws UnsupportedGenericException{
//		acquireWriteLock();
//		try {
			keys.add(key);
			values.add(new ArrayList<TValue>(Arrays.asList(value)));
			++keyCount;
//		} finally {
//			releaseWriteLock();
//		}

	}

	public void insertKeyValueInBulkLoading(TKey key, TValue value) throws UnsupportedGenericException{
		keys.add(key);
		values.add(new ArrayList<TValue>(Arrays.asList(value)));
		++keyCount;
	}

	/*

    public List<TValue> searchRangeInTemplate(TKey leftKey, TKey rightKey) {
		int firstIndex;
		BTreeLeafNode<TKey,TValue> currLeaf = this;
		Lock lastLock = this.getrLock();

		while (currLeaf.rightSibling != null && currLeaf.getKeyCount() == 0) {
			if (currLeaf.rightSibling != null) {
				currLeaf.rightSibling.acquireReadLock();
				lastLock.unlock();
				lastLock = currLeaf.rightSibling.getrLock();
			}
			currLeaf = (BTreeLeafNode<TKey,TValue>) currLeaf.rightSibling;
		}
//		shiftToLeftMostNotEmptyRightSibling(currLeaf, lastLock);
		for (firstIndex = 0; firstIndex < currLeaf.getKeyCount(); firstIndex++) {
			int cmp = currLeaf.getKey(firstIndex).compareTo(leftKey);
			if (cmp >= 0)
				break;
		}
		List<TValue> retList = new ArrayList<TValue>();
		int currIndex = firstIndex;
		// case when all keys in the node are smaller than leftKey - shift to next rightSibling
		if (firstIndex >= currLeaf.getKeyCount()) {
			currLeaf = (BTreeLeafNode<TKey,TValue>) currLeaf.rightSibling;
			if (currLeaf != null) {
				currLeaf.acquireReadLock();
				if (lastLock != null) {
					lastLock.unlock();
				}
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
//			shiftToLeftMostNotEmptyRightSibling(currLeaf, lastLock);
			currIndex = 0;
		}
		while (currLeaf != null && currLeaf.getKey(currIndex).compareTo(rightKey) <= 0) {
//			currLeaf.print();
//			if (currRightSibling != currLeaf) {
//				System.out.println(false);
//			}
//			assert currLeaf != null;
//			if (currLeaf.lock.getReadLockCount() == 0) {
//				System.out.println(currIndex);
//				currLeaf.leftSibling.print();
//				currLeaf.print();
//				currLeaf.rightSibling.print();
//			}
//			assert currLeaf.lock.getReadLockCount() > 0;
			retList.addAll(currLeaf.getValueList(currIndex));
			currIndex++;
			if (currIndex >= currLeaf.getKeyCount()) {

				if (currLeaf.rightSibling != null) {
//					currRightSibling = currLeaf.rightSibling;
					currLeaf.rightSibling.acquireReadLock();
//                    nodes.add((currLeaf.rightSibling));
					if (lastLock != null) {
//						System.out.println(currIndex);
//						System.out.println(this.writeLockThreadId);
						lastLock.unlock();
					}
					lastLock = currLeaf.rightSibling.getrLock();
				}
				currLeaf = (BTreeLeafNode<TKey,TValue>) currLeaf.rightSibling;
				while (currLeaf != null && currLeaf.getKeyCount() == 0) {
					if (currLeaf.rightSibling != null) {
						currLeaf.rightSibling.acquireReadLock();
						lastLock.unlock();
						lastLock = currLeaf.rightSibling.getrLock();
					}
					currLeaf = (BTreeLeafNode<TKey,TValue>) currLeaf.rightSibling;
				}
//				shiftToLeftMostNotEmptyRightSibling(currLeaf, lastLock);
				currIndex = 0;
			}
		}
		if (lastLock != null) {
			lastLock.unlock();
		}
		return retList;
    }

    */




	public List<byte[]> searchRangeInTemplate(TKey leftKey, TKey rightKey) {
		int firstIndex;
		BTreeLeafNode<TKey,TValue> currLeaf = this;
		Lock lastLock = this.getrLock();

		while (currLeaf.rightSibling != null && currLeaf.getKeyCount() == 0) {
			if (currLeaf.rightSibling != null) {
				currLeaf.rightSibling.acquireReadLock();
				lastLock.unlock();
				lastLock = currLeaf.rightSibling.getrLock();
			}
			currLeaf = (BTreeLeafNode<TKey,TValue>) currLeaf.rightSibling;
		}
//		shiftToLeftMostNotEmptyRightSibling(currLeaf, lastLock);
		for (firstIndex = 0; firstIndex < currLeaf.getKeyCount(); firstIndex++) {
			int cmp = currLeaf.getKey(firstIndex).compareTo(leftKey);
			if (cmp >= 0)
				break;
		}
		List<byte[]> retList = new ArrayList<byte[]>();
		int currIndex = firstIndex;
		// case when all keys in the node are smaller than leftKey - shift to next rightSibling
		if (firstIndex >= currLeaf.getKeyCount()) {
			currLeaf = (BTreeLeafNode<TKey,TValue>) currLeaf.rightSibling;
			if (currLeaf != null) {
				currLeaf.acquireReadLock();
				if (lastLock != null) {
					lastLock.unlock();
				}
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
//			shiftToLeftMostNotEmptyRightSibling(currLeaf, lastLock);
			currIndex = 0;
		}
		while (currLeaf != null && currLeaf.getKey(currIndex).compareTo(rightKey) <= 0) {
//			currLeaf.print();
//			if (currRightSibling != currLeaf) {
//				System.out.println(false);
//			}
//			assert currLeaf != null;
//			if (currLeaf.lock.getReadLockCount() == 0) {
//				System.out.println(currIndex);
//				currLeaf.leftSibling.print();
//				currLeaf.print();
//				currLeaf.rightSibling.print();
//			}
//			assert currLeaf.lock.getReadLockCount() > 0;
			retList.addAll(currLeaf.getTuples(currIndex));
			currIndex++;
			if (currIndex >= currLeaf.getKeyCount()) {

				if (currLeaf.rightSibling != null) {
//					currRightSibling = currLeaf.rightSibling;
					currLeaf.rightSibling.acquireReadLock();
//                    nodes.add((currLeaf.rightSibling));
					if (lastLock != null) {
//						System.out.println(currIndex);
//						System.out.println(this.writeLockThreadId);
						lastLock.unlock();
					}
					lastLock = currLeaf.rightSibling.getrLock();
				}
				currLeaf = (BTreeLeafNode<TKey,TValue>) currLeaf.rightSibling;
				while (currLeaf != null && currLeaf.getKeyCount() == 0) {
					if (currLeaf.rightSibling != null) {
						currLeaf.rightSibling.acquireReadLock();
						lastLock.unlock();
						lastLock = currLeaf.rightSibling.getrLock();
					}
					currLeaf = (BTreeLeafNode<TKey,TValue>) currLeaf.rightSibling;
				}
//				shiftToLeftMostNotEmptyRightSibling(currLeaf, lastLock);
				currIndex = 0;
			}
		}
		if (lastLock != null) {
			lastLock.unlock();
		}
		return retList;
	}





    private void shiftToLeftMostNotEmptyRightSibling(BTreeLeafNode currLeaf, Lock lastLock) {
		while (currLeaf != null && currLeaf.getKeyCount() == 0) {
			if (currLeaf.rightSibling != null) {
				currLeaf.rightSibling.acquireReadLock();
				lastLock.unlock();
				lastLock = currLeaf.rightSibling.getrLock();
			}
			currLeaf = (BTreeLeafNode<TKey,TValue>) currLeaf.rightSibling;
		}
	}

	public void addBytesCount(int len) {
		bytesCount += len;
	}

	public void substactBytesCount(int len) {
		bytesCount -= len;
	}

	public ArrayList<byte[]> rangeSearchAndGetTuples(TKey leftKey, TKey rightKey) {
		ArrayList<byte[]> tuples = new ArrayList<byte[]>();
		for (int i = 0; i < keys.size(); ++i) {
			if (keys.get(i).compareTo(leftKey) >= 0 && keys.get(i).compareTo(rightKey) <= 0) {
				tuples.addAll(getTuples(i));
			}
		}
		return tuples;
	}

}
