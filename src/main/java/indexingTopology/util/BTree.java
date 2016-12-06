package indexingTopology.util;

import indexingTopology.Config.Config;
import indexingTopology.exception.UnsupportedGenericException;
import org.omg.Messaging.SYNC_WITH_TRANSPORT;

import java.io.*;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * A B+ tree
 * Since the structures and behaviors between internal node and external node are different, 
 * so there are two different classes for each kind of node.
 * @param <TKey> the data type of the key
 */
public class BTree <TKey extends Comparable<TKey>,TValue> implements Serializable{
	private volatile BTreeNode<TKey> root;
	//  private final BytesCounter counter;
	private BytesCounter counter;
	private TimingModule tm;
	private boolean templateMode;
	private SplitCounterModule sm;

//	private final ReadWriteLock lock;
//	private final Lock wLock;
//	private final Lock rLock;

	public BTree(int order, TimingModule tm, SplitCounterModule sm) {
		counter = new BytesCounter();
		this.root = new BTreeLeafNode<TKey,TValue>(order,counter);
		counter.increaseHeightCount();
		templateMode = false;
//		sem = new Semaphore(1);
//		this.lock = new ReentrantReadWriteLock();
//		this.wLock = lock.writeLock();
//		this.rLock = lock.readLock();

		assert tm != null : "Timing module cannot be null";
		assert sm != null : "Split counter module cannot be null";
		this.tm = tm;
		this.sm = sm;
	}

	public BTree(int order, TimingModule tm, SplitCounterModule sm, boolean templateMode) {
		this(order, tm, sm);
		this.templateMode = templateMode;
	}

	public BTreeNode getRoot() {
		return root;
	}

	public BTree(BTree bt) throws CloneNotSupportedException{
		this.counter = (BytesCounter) bt.counter.clone();
		this.root = (BTreeNode) bt.root.clone(bt.root);
		setTimingModule(bt.tm);
		setSplitCounterModule(bt.sm);
		templateMode = bt.templateMode;

//		this.lock = new ReentrantReadWriteLock();
//		this.wLock = lock.writeLock();
//		this.rLock = lock.readLock();
	}

	public void setRoot(BTreeNode root) {
		this.root = root;
	}

	public void setCounter(BytesCounter counter) { this.counter = counter; }

	public void setTimingModule(TimingModule tm) {
		this.tm = tm;
	}

	public void setSplitCounterModule(SplitCounterModule sm) {
		this.sm = sm;
	}

	public int getTotalBytes() {
		return counter.getBytesCount();
	}

	public int getBytesEstimateForInsert(TKey key,byte [] value) throws UnsupportedGenericException {
		if (!templateMode)
			return counter.getBytesEstimateForInsert(UtilGenerics.sizeOf(key.getClass()), value.length);
		else
			return counter.getBytesEstimateForInsertInTemplate();
	}
    /*
	public byte[] serializeTree() {
		ByteBuffer b=ByteBuffer.allocate(getTotalBytes());
		Queue<BTreeNode<TKey>> q = new LinkedList<BTreeNode<TKey>>();
		q.add(root);
		while (!q.isEmpty()) {
			BTreeNode<TKey> curr=q.remove();
			Collection<BTreeNode<TKey>> children=curr.recursiveSerialize(b);
			if (children!=null)
				q.addAll(children);
		}

		return b.array();
	}*/

    public byte[] serializeTree() {
        ByteBuffer b = ByteBuffer.allocate(Config.TEMPLATE_SIZE);
//		byte[] lengthOfSerializedTreeInByte = ByteBuffer.allocate(Integer.SIZE / Byte.SIZE).
//				putInt(counter.getBytesEstimateForInsertInTemplate()).array();
//		b.put(lengthOfSerializedTreeInByte);
//		ByteBuffer b = ByteBuffer.allocate(640000);
        Queue<BTreeNode<TKey>> q = new LinkedList<BTreeNode<TKey>>();
        q.add(root);
        while (!q.isEmpty()) {
            BTreeInnerNode<TKey> curr = (BTreeInnerNode) q.remove();
            b.put(curr.serialize());
            if (curr.children.size() > 0 && curr.getChild(0).getNodeType() == TreeNodeType.InnerNode) {
				q.addAll(curr.children);
			}
        }
        return b.array();
    }

    public BTree deserializeTree(byte[] serializedTree, int BTreeOrder, BytesCounter counter) {
		BTreeInnerNode root = null;
		if (serializedTree.length > 0) {
			Queue<BTreeNode<TKey>> q = new LinkedList<BTreeNode<TKey>>();
			int relativeOffset = 0;
			int len = Integer.SIZE / Byte.SIZE;
			root = new BTreeInnerNode(BTreeOrder, counter);
			relativeOffset = root.deserialize(serializedTree, root, relativeOffset);
			root.print();
			q.add(root);
			while (!q.isEmpty()) {
				BTreeInnerNode<TKey> curr = (BTreeInnerNode) q.remove();
				for (int i = 0; i < curr.getKeyCount() + 1; ++i) {
					BTreeInnerNode node = new BTreeInnerNode(BTreeOrder, counter);
					relativeOffset = root.deserialize(serializedTree, node, relativeOffset);
					node.print();
					curr.setChild(i, node);
					if (curr.offsets.size() != 0) {
						q.add(node);
					}
				}
			}
		}
		TimingModule tm = TimingModule.createNew();
		SplitCounterModule sm = SplitCounterModule.createNew();
		BTree bTree = new BTree(BTreeOrder, tm, sm);
		bTree.setRoot(root);
		return bTree;
	}

	/**
	 * Insert a new key and its associated value into the B+ tree.
	 * return true if
	 */
	/*
	public void insert(TKey key, TValue value) throws UnsupportedGenericException {
		BTreeLeafNode<TKey, TValue> leaf = null;
//		rLock.lock();
//		try {
			long start = System.nanoTime();
			leaf = this.findLeafNodeShouldContainKey(key);
			long time = System.nanoTime() - start;
			tm.putDuration(Constants.TIME_LEAF_FIND.str, time);

//		} finally {
//			rLock.unlock();
//		}
//		for (int i=0;i<1000;i++) {
//			tm.startTiming(Constants.TIME_LEAF_FIND.str);
//			leaf = this.findLeafNodeShouldContainKey(key);
//			tm.endTiming(Constants.TIME_LEAF_FIND.str);
//		}
//		wLock.lock();
//		try {
			start = System.nanoTime();
			leaf.insertKeyValue(key, value);
			time = System.nanoTime() - start;
//			tm.putDuration(Constants.TIME_LEAF_INSERTION.str, time);


//		for (int i=0;i<1000;i++) {
//			tm.startTiming(Constants.TIME_LEAF_INSERTION.str);
//			leaf.insertKeyValue(key,value);
//			tm.endTiming(Constants.TIME_LEAF_INSERTION.str);
//			leaf.deleteKeyValue(key,value);
//		}
			if (templateMode && leaf.isOverflow()) {
				//		tm.putDuration(Constants.TIME_SPLIT.str, 0);
				sm.addCounter();
				//
			} else if (!leaf.isOverflow()) {
//			if (!leaf.isOverflow()) {
//				tm.putDuration(Constants.TIME_SPLIT.str, 0);

//		} else {
//        if (templateMode || !leaf.isOverflow()) {
//            tm.putDuration(Constants.TIME_SPLIT.str, 0);

			} else {
				start = System.nanoTime();
				BTreeNode<TKey> n = leaf.dealOverflow();
				if (n != null) {
					this.root = n;
					time = System.nanoTime() - start;
					tm.putDuration(Constants.TIME_SPLIT.str, time);
				} else {
					System.out.println("the root is null");
				}
			}
//		} finally {
//			wLock.unlock();
//		}
	}
	*/

	/**
	 * insert the key and value to the B+ tree
	 * based on the mode of the tree, the function will choose the corresponding protocol
	 * @param key the index value
	 * @param value  the offset
	 * @throws UnsupportedGenericException
	 */

	public void insert(TKey key, TValue value) throws UnsupportedGenericException {
        BTreeLeafNode<TKey, TValue> leaf = null;
        if (templateMode) {
//            System.out.println("templateMode");
            leaf = findLeafNodeShouldContainKeyInTemplate(key);
//            leaf.acquireWriteLock();
//            try {
                leaf.insertKeyValueInTemplateMode(key, value);
                if (leaf.isOverflow()) {
                    sm.addCounter();
                }
//            } finally {
//                leaf.releaseWriteLock();
//            }
        } else {
            leaf = findLeafNodeShouldContainKeyInUpdaterWithProtocolTwo(key);
            ArrayList<BTreeNode> ancestors = new ArrayList<BTreeNode>();
            //if the root is null, it means that we have to use protocol 1 instead of protocol 2.
            if (leaf == null) {
                leaf = findLeafNodeShouldContainKeyInUpdaterWithProtocolOne(key, ancestors);
            }
            BTreeNode root = leaf.insertKeyValue(key, value);
            if (root != null) {
                this.setRoot(root);
            }
            leaf.releaseWriteLock();
            for (BTreeNode ancestor : ancestors) {
                ancestor.releaseWriteLock();
            }
            ancestors.clear();
        }
	}


	public void insert(TKey key, byte[] serializedTuple) throws UnsupportedGenericException {
		BTreeLeafNode<TKey, TValue> leaf = null;
		if (templateMode) {
//            System.out.println("templateMode");
			leaf = findLeafNodeShouldContainKeyInTemplate(key);
//            leaf.acquireWriteLock();
//            try {
			leaf.insertKeyValueInTemplateMode(key, serializedTuple);
			if (leaf.isOverflow()) {
				sm.addCounter();
			}
//            } finally {
//                leaf.releaseWriteLock();
//            }
		} else {
			leaf = findLeafNodeShouldContainKeyInUpdaterWithProtocolTwo(key);
			ArrayList<BTreeNode> ancestors = new ArrayList<BTreeNode>();
			//if the root is null, it means that we have to use protocol 1 instead of protocol 2.
			if (leaf == null) {
				leaf = findLeafNodeShouldContainKeyInUpdaterWithProtocolOne(key, ancestors);
			}
			BTreeNode root = leaf.insertKeyValue(key, serializedTuple);
			if (root != null) {
				this.setRoot(root);
			}
			leaf.releaseWriteLock();
			for (BTreeNode ancestor : ancestors) {
				ancestor.releaseWriteLock();
			}
			ancestors.clear();
		}
	}



	/**
	 * TODO what happens if same key different value
	 * Search a key value on the tree and return its associated value.
	 */
/*	public ArrayList<TValue> search(TKey key) {
		BTreeLeafNode<TKey,TValue> leaf = this.findLeafNodeShouldContainKey(key);
		int index = leaf.search(key);
		return (index == -1) ? null : leaf.getValueList(index);
	}*/

//	public ArrayList<TValue> search(TKey key) {
//		ArrayList<TValue> values = null;
//		rLock.lock();
//		try {
//			BTreeLeafNode<TKey, TValue> leaf = this.findLeafNodeShouldContainKey(key);
//		    values = leaf.searchAndGetValues(key);   //Add the method searchAndGetValues to check the lock;
//			int index = leaf.searchAndGetValues(key);
//			return (index == -1) ? null : leaf.getValueList(index);
//			values = (index == -1) ? null : leaf.getValueList(index);
//		} finally {
//			rLock.unlock();
//		}
//		return values;
//	}

//	public ArrayList<TValue> search(TKey key) {
//		ArrayList<TValue> values = null;
//		BTreeLeafNode<TKey, TValue> leaf = this.findLeafNodeShouldContainKeyInReader(key);
//		values = leaf.searchAndGetValues(key);   //Add the method searchAndGetValues to check the lock;
//		return values;
//	}

	//The method below are changed to check the paper which is about concurrency in B tree

	/**
	 * search operation for the reader
	 * @param key
	 * @return values of the corresponding key.
	 */
	public ArrayList<TValue> search(TKey key) {
		ArrayList<TValue> values = null;
		BTreeLeafNode<TKey, TValue> leaf = null;
		if (!templateMode) {
			leaf = this.findLeafNodeShouldContainKeyInReader(key);
			values = leaf.searchAndGetValues(key);
			leaf.releaseReadLock();
		} else {
			leaf = this.findLeafNodeShouldContainKeyInTemplate(key);
            leaf.acquireReadLock();
            try {
                values = leaf.searchAndGetValuesInTemplate(key);
            } finally {
                leaf.releaseReadLock();
            }
        }
		return values;
	}

	public ArrayList<byte[]> searchTuples(TKey key) {
		ArrayList<byte[]> tuples = null;
		BTreeLeafNode<TKey, TValue> leaf = null;
		if (!templateMode) {
			leaf = this.findLeafNodeShouldContainKeyInReader(key);
			tuples = leaf.searchAndGetTuples(key);
			leaf.releaseReadLock();
		} else {
			leaf = this.findLeafNodeShouldContainKeyInTemplate(key);
			leaf.acquireReadLock();
			try {
				tuples = leaf.searchAndGetTuplesInTemplate(key);
			} finally {
				leaf.releaseReadLock();
			}
		}
		return tuples;
	}





    /*
	//The method below are changed to check the paper which is about concurrency in B tree
	public List<TValue> searchRange(TKey leftKey, TKey rightKey) {
		assert leftKey.compareTo(rightKey) <= 0 : "leftKey provided is greater than the right key";
        List<TValue> values = null;
        if (!templateMode) {
            BTreeLeafNode<TKey, TValue> leafLeft = this.findLeafNodeShouldContainKeyInReader(leftKey);
            try {
//            List<TValue> values = leafLeft.searchRange(leftKey, rightKey);
                values = leafLeft.searchRange(leftKey, rightKey);
            } catch (IndexOutOfBoundsException e) {
//            System.out.println("Debug: " + Thread.currentThread().getId() + "Out of bounds exception");
            }
        } else {
            BTreeLeafNode<TKey, TValue> leafLeft = this.findLeafNodeShouldContainKeyInTemplate(leftKey);
            leafLeft.acquireReadLock();
            try {
//            List<TValue> values = leafLeft.searchRange(leftKey, rightKey);
                values = leafLeft.searchRangeInTemplate(leftKey, rightKey);
            } catch (IndexOutOfBoundsException e) {
//            System.out.println("Debug: " + Thread.currentThread().getId() + "Out of bounds exception");
            }
        }
		return values;
	}
	*/


	public List<byte[]> searchRange(TKey leftKey, TKey rightKey) {
		assert leftKey.compareTo(rightKey) <= 0 : "leftKey provided is greater than the right key";
		List<byte[]> tuples = null;
		if (!templateMode) {
			BTreeLeafNode<TKey, TValue> leafLeft = this.findLeafNodeShouldContainKeyInReader(leftKey);
			try {
//            List<TValue> values = leafLeft.searchRange(leftKey, rightKey);
				tuples = leafLeft.searchRange(leftKey, rightKey);
			} catch (IndexOutOfBoundsException e) {
//            System.out.println("Debug: " + Thread.currentThread().getId() + "Out of bounds exception");
			}
		} else {
			BTreeLeafNode<TKey, TValue> leafLeft = this.findLeafNodeShouldContainKeyInTemplate(leftKey);
			leafLeft.acquireReadLock();
			try {
//            List<TValue> values = leafLeft.searchRange(leftKey, rightKey);
				tuples = leafLeft.searchRangeInTemplate(leftKey, rightKey);
			} catch (IndexOutOfBoundsException e) {
//            System.out.println("Debug: " + Thread.currentThread().getId() + "Out of bounds exception");
			}
		}
		return tuples;
	}





	/**
	 * Delete a key and its associated value from the tree. TODO Fix.might have a bug.
	 */
//	public void delete(TKey key) {
//		BTreeLeafNode<TKey,TValue> leaf = this.findLeafNodeShouldContainKey(key);
//
//		if (leaf.delete(key) && leaf.isUnderflow()) {
//			BTreeNode<TKey> n = leaf.dealUnderflow();
//			if (n != null)
//				this.root = n;
//		}
//	}
	public void delete(TKey key) {
		BTreeLeafNode<TKey, TValue> leaf = null;
        leaf = findLeafNodeShouldContainKeyInUpdaterWithProtocolTwo(key);
				//if the root is null means that we need to give up using protocol 2 and use protocol 1.
        ArrayList<BTreeNode> ancestors = new ArrayList<BTreeNode>();
        if (leaf == null) {
            leaf = findLeafNodeShouldContainKeyInUpdaterWithProtocolOne(key, ancestors);
        }
        if (leaf.delete(key) && leaf.isUnderflow()) {
            BTreeNode<TKey> root = leaf.dealUnderflow();
            if (root != null)
                this.setRoot(root);
        }
        leaf.releaseWriteLock();
        for (BTreeNode ancestor : ancestors) {
            ancestor.releaseWriteLock();
        }
        ancestors.clear();
	}

	/**
	 * Search the leaf node which should contain the specified key
	 */
	@SuppressWarnings("unchecked")
	/*
	private BTreeLeafNode<TKey,TValue> findLeafNodeShouldContainKey(TKey key) {

		BTreeNode<TKey> node = this.root;
		while (node.getNodeType() == TreeNodeType.InnerNode) {
			node = ((BTreeInnerNode<TKey>) node).getChild(node.search(key));
		}
		return (BTreeLeafNode<TKey,TValue>)node;

	} */

	public BTreeLeafNode<TKey,TValue> findLeafNodeShouldContainKeyInTemplate(TKey key) {
		BTreeNode<TKey> currentNode = this.root;
		while (currentNode.getNodeType() == TreeNodeType.InnerNode) {
			BTreeNode<TKey> node = ((BTreeInnerNode<TKey>) currentNode).getChild(currentNode.search(key));
			currentNode = node;
		}
		return (BTreeLeafNode<TKey,TValue>) currentNode;
	}

	/**
	 * Protocol for the reader to find the leaf node that should contain key
	 * @param key
	 * @return the leaf node should contain the key
	 */

	private BTreeLeafNode<TKey,TValue> findLeafNodeShouldContainKeyInReader(TKey key) {

        BTreeNode tmpRoot = root;
        root.acquireReadLock();
        while (tmpRoot != root) {
            tmpRoot.releaseReadLock();
            tmpRoot = root;
            root.acquireReadLock();
        }

        BTreeNode<TKey> currentNode = this.root;
        while (currentNode.getNodeType() == TreeNodeType.InnerNode) {
            BTreeNode<TKey> node = ((BTreeInnerNode<TKey>) currentNode).getChildWithSpecificIndex(key);
            node.acquireReadLock();
            currentNode.releaseReadLock();
            currentNode = node;
        }
        return (BTreeLeafNode<TKey,TValue>) currentNode;

	}





	/**
	 * Protocol 2 for the updater to find the leaf node that should contain key
	 * @param key
	 * @return the leaf node that contains the key
	 */

	private BTreeLeafNode<TKey,TValue> findLeafNodeShouldContainKeyInUpdaterWithProtocolTwo(TKey key) {
        BTreeNode tmpRoot = root;
        root.acquireReadLock();
        while (tmpRoot != root) {
            tmpRoot.releaseReadLock();
            tmpRoot = root;
            root.acquireReadLock();
        }

//        BTreeNode<TKey> currentRoot;
//        Lock lastLock = null;
//        do {
//            if(lastLock!=null)
//                lastLock.unlock();
//            currentRoot = root;
//            currentRoot.acquireReadLock();
//            lastLock = currentRoot.getrLock();
//        } while(currentRoot!=root);

        if (getHeight() == 1) {
            tmpRoot.releaseReadLock();
            return null;
        }

        BTreeNode<TKey> currentNode = this.root;
        while (currentNode.getNodeType() == TreeNodeType.InnerNode) {
            BTreeNode<TKey> node = ((BTreeInnerNode<TKey>) currentNode).getChildWithSpecificIndex(key);
            if (node.getNodeType() == TreeNodeType.InnerNode) {
                node.acquireReadLock();
            } else {
                node.acquireWriteLock();
            }
            currentNode.releaseReadLock();
            currentNode = node;
        }
        if (!currentNode.isSafe()) {
            currentNode.releaseWriteLock();
            return null;
        }
        return (BTreeLeafNode<TKey,TValue>) currentNode;
	}

	/**
	 * Protocol 1 for the updater to find the leaf node that should contain key
	 * @param key
	 * @param ancestorsOfCurrentNode record the ancestor of the node which needs to be split
	 * @return the leaf node
	 */

	private BTreeLeafNode<TKey,TValue> findLeafNodeShouldContainKeyInUpdaterWithProtocolOne(TKey key, List<BTreeNode> ancestorsOfCurrentNode) {
        BTreeNode tmpRoot = root;
        root.acquireWriteLock();
        while (tmpRoot != root) {
            tmpRoot.releaseWriteLock();
            tmpRoot = root;
            root.acquireWriteLock();
        }

//        BTreeNode<TKey> currentRoot;
//        Lock lastLock = null;
//        do {
//            if(lastLock != null)
//                lastLock.unlock();
//            currentRoot = root;
//            currentRoot.acquireWriteLock();
//            lastLock = currentRoot.getwLock();
//        } while(currentRoot != root);

        BTreeNode<TKey> currentNode = this.root;
        while (currentNode.getNodeType() == TreeNodeType.InnerNode) {
            BTreeNode<TKey> node = ((BTreeInnerNode<TKey>) currentNode).getChildWithSpecificIndex(key);
            ancestorsOfCurrentNode.add(currentNode);
            node.acquireWriteLock();
            if (node.isSafe()) {
                for (BTreeNode ancestor : ancestorsOfCurrentNode) {
                    ancestor.releaseWriteLock();
                }
                ancestorsOfCurrentNode.clear();
            }
            currentNode = node;
        }
		return (BTreeLeafNode<TKey,TValue>) currentNode;

	}
	private BTreeLeafNode<TKey,TValue> findLeafNodeShouldContainKey(TKey key) {
		BTreeNode<TKey> currentNode = this.root;
		while (currentNode.getNodeType() == TreeNodeType.InnerNode) {
//			currentNode = ((BTreeInnerNode<TKey>) currentNode).getChildWithSpecificIndex(key);
//			currentNode = ((BTreeInnerNode<TKey>) currentNode).getChild(currentNode.search(key));
		}
		return (BTreeLeafNode) currentNode;
	}


	/*  method to keep tree template intact, while just removing the tree data payload
	 */
	public void clearPayload() {
		templateMode = true;
		Queue<BTreeNode<TKey>> q = new LinkedList<BTreeNode<TKey>>();
		q.add(this.root);
		while (!q.isEmpty()) {
			BTreeNode<TKey> curr = q.remove();
			if (curr.getNodeType().equals(TreeNodeType.LeafNode)) {
				((BTreeInnerNode) curr.getParent()).offsets.clear();
				((BTreeLeafNode) curr).clearNode();

			} else {
				q.addAll(((BTreeInnerNode) curr).children);
			}
		}
	}


	public void printBtree() {
		Queue<BTreeNode<TKey>> q = new LinkedList<BTreeNode<TKey>>();
		//	int height = 0;
		//	int numberOfLeaves = 0;
		//	List<TKey> list = new LinkedList<TKey>();
		q.add(root);
		while (!q.isEmpty()) {
			//	++height;
			Queue<BTreeNode<TKey>> qInner = new LinkedList<BTreeNode<TKey>>();
			//	list = new LinkedList<TKey>();
			//	numberOfLeaves = 0;
			while (!q.isEmpty()) {
				BTreeNode<TKey> curr = q.remove();
				//		++numberOfLeaves;
				if (curr.getNodeType().equals(TreeNodeType.InnerNode)) {
					qInner.addAll(((BTreeInnerNode) curr).children);
				}
				for (TKey k : curr.keys) {
					//				list.add(k);
					System.out.print(k + " ");
				}

				System.out.print(": ");
			}

			System.out.println();
			q = qInner;
			//	if (q.isEmpty()) {
			//		System.out.println("The number of leaves of the tree is " + numberOfLeaves);
			//	}
		}
		//	System.out.println("The height of BTree is " + height);
		//	return list;
	}


	public Object clone(BTree bt) throws CloneNotSupportedException{
		BTree newBtree = new BTree(bt);
		return newBtree;
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

	public void setHeight(int height) {
		this.counter.setHeight(height);
	}

	public int getHeight() {
		return counter.getHeightCount();
	}

	public boolean validateParanetReference() {
		return root.validateParentReference();
	}

	public boolean validateNoDuplicatedChildReference() {
		return root.validateNoDuplicatedChildReference();
	}

	public boolean validateAllLockReleased() {
		return root.validateAllLockReleased();
	};

	public void printStatistics() {
		System.out.println("Depth: " + root.getDepth());
	}

	public BTreeLeafNode getLeftMostLeaf() {
		BTreeNode<TKey> currentNode = this.root;
		while (currentNode.getNodeType() == TreeNodeType.InnerNode) {
			BTreeNode<TKey> node = ((BTreeInnerNode<TKey>) currentNode).getChild(0);
			currentNode = node;
		}
		return (BTreeLeafNode<TKey,TValue>) currentNode;
	}

	public void setTemplateMode() {
		templateMode = true;
	}

	public void writeLeavesIntoChunk(MemChunk chunk) {
		BTreeLeafNode leave = getLeftMostLeaf();
		int count = 0;
		int offset = 0;
		while (leave != null) {
			++count;
//			byte[] serializedLeave = leave.serialize();
			byte[] serializedLeave = SerializationHelper.serializeLeafNode(leave);
//			leave.print();
            offset = chunk.write(serializedLeave);
//			System.out.println("Offset" + offset);
            ((BTreeInnerNode)leave.getParent()).putOffset(offset);
            leave = (BTreeLeafNode) leave.rightSibling;
		}
//		System.out.println("Offset :" + chunk.getData().position());
	}

	public BytesCounter getCounter() {
		return counter;
	}

	public int getOffsetOfLeaveNodeShouldContainKey(TKey key) {
		BTreeInnerNode<TKey> currentNode = (BTreeInnerNode) this.root;
		while (currentNode.children.size() > 0) {
			BTreeNode<TKey> node = ((BTreeInnerNode<TKey>) currentNode).getChildWithSpecificIndex(key);
			currentNode = (BTreeInnerNode) node;
		}
		return currentNode.offsets.get(currentNode.search(key));
	}

	public BTreeNode findLeafNodeShouldContainKeyInDeserializedTemplate(TKey key) {
		BTreeInnerNode<TKey> currentNode = (BTreeInnerNode) this.root;
		while (currentNode.children.size() > 0) {
			BTreeNode<TKey> node = ((BTreeInnerNode<TKey>) currentNode).getChildWithSpecificIndex(key);
			currentNode = (BTreeInnerNode) node;
		}
		return (BTreeNode) currentNode;
	}

	public List<Integer> getOffsetsOfLeaveNodesShoulsContainKeys(BTreeNode mostLeftNode
			, BTreeNode mostRightNode) {
		List<Integer> offsets = new ArrayList<Integer>();
		BTreeInnerNode<TKey> currentNode = (BTreeInnerNode) mostLeftNode;
		while (currentNode != mostRightNode) {
//			currentNode.print();
			offsets.addAll(currentNode.offsets);
			currentNode = (BTreeInnerNode) currentNode.rightSibling;
		}
		offsets.addAll(((BTreeInnerNode) mostRightNode).offsets);
		return offsets;
	}
}

