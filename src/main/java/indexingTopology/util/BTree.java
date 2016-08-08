package indexingTopology.util;

import indexingTopology.exception.UnsupportedGenericException;

import java.io.*;
import java.nio.ByteBuffer;
import java.util.*;
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
	private BTreeNode<TKey> root;
  //  private final BytesCounter counter;
	private BytesCounter counter;
	private TimingModule tm;
	private boolean templateMode;
    private SplitCounterModule sm;
	private ReadWriteLock rwl;
	private Lock readLock;
	private Lock writeLock;
	
	public BTree(int order, TimingModule tm, SplitCounterModule sm) {
		counter = new BytesCounter();
        this.root = new BTreeLeafNode<TKey,TValue>(order,counter);
        counter.increaseHeightCount();
		templateMode = false;
        rwl = new ReentrantReadWriteLock();
        readLock = rwl.readLock();
		writeLock = rwl.writeLock();

		assert tm != null : "Timing module cannot be null";
		assert sm != null : "Split counter module cannot be null";
		this.tm = tm;
		this.sm = sm;
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
			return counter.getBytesEstimateForInsertInTemplate(UtilGenerics.sizeOf(key.getClass()), value.length);
    }

    public byte[] serializeTree() {
        ByteBuffer b=ByteBuffer.allocate(getTotalBytes());
        Queue<BTreeNode<TKey>> q=new LinkedList<BTreeNode<TKey>>();
        q.add(root);
        while (!q.isEmpty()) {
            BTreeNode<TKey> curr=q.remove();
            Collection<BTreeNode<TKey>> children=curr.recursiveSerialize(b);
            if (children!=null)
                q.addAll(children);
        }

        return b.array();
    }

	/**
	 * Insert a new key and its associated value into the B+ tree.
     * return true if
	 */
	public void insert(TKey key, TValue value) throws UnsupportedGenericException {
		BTreeLeafNode<TKey, TValue> leaf = null;
	//	System.out.println("Insert has been called");
		long start = System.nanoTime();
//			tm.startTiming(Constants.TIME_LEAF_FIND.str);
		leaf = this.findLeafNodeShouldContainKey(key);
//			tm.endTiming(Constants.TIME_LEAF_FIND.str);

		long time = System.nanoTime() - start;
		tm.putDuration(Constants.TIME_LEAF_FIND.str, time);

	//	System.out.println(tm.getFindTime());


//		for (int i=0;i<1000;i++) {
//			tm.startTiming(Constants.TIME_LEAF_FIND.str);
//			leaf = this.findLeafNodeShouldContainKey(key);
//			tm.endTiming(Constants.TIME_LEAF_FIND.str);
//		}
		start = System.nanoTime();
	//	tm.startTiming(Constants.TIME_LEAF_INSERTION.str);
//		synchronized (leaf) {
			leaf.insertKeyValue(key, value);
//		}

		time = System.nanoTime() - start;
	//	tm.endTiming(Constants.TIME_LEAF_INSERTION.str);
		tm.putDuration(Constants.TIME_LEAF_INSERTION.str, time);



//		for (int i=0;i<1000;i++) {
//			tm.startTiming(Constants.TIME_LEAF_INSERTION.str);
//			leaf.insertKeyValue(key,value);
//			tm.endTiming(Constants.TIME_LEAF_INSERTION.str);
//			leaf.deleteKeyValue(key,value);
//		}
     //   if (templateMode && leaf.isOverflow()) {
	//		tm.putDuration(Constants.TIME_SPLIT.str, 0);
	//		sm.addCounter();
	//	}
	//	//else if (!leaf.isOverflow()) {
		if(!leaf.isOverflow()) {
			tm.putDuration(Constants.TIME_SPLIT.str, 0);
		} else {
     //   if (templateMode || !leaf.isOverflow()) {
     //       tm.putDuration(Constants.TIME_SPLIT.str, 0);

     //   } else {
			start = System.nanoTime();
		//	tm.startTiming(Constants.TIME_SPLIT.str);
			BTreeNode<TKey> n = leaf.dealOverflow(sm, leaf);
				//	tm.endTiming(Constants.TIME_SPLIT.str);
			if (n != null) {
				this.root = n;
				time = System.nanoTime() - start;
				tm.putDuration(Constants.TIME_SPLIT.str, time);
			//	tm.endTiming(Constants.TIME_SPLIT.str);
			} else {
				System.out.println("the root is null");
			}
        }
	//	int numberOfSplit = sm.getSplitTimeOnLeaf(leaf);
	//	sm.reset(leaf);
	//	sm.traverseSplit();
	//	sm.reset(leaf);
	//	return numberOfSplit;
	}

	/**
	 * TODO what happens if same key different value
	 * Search a key value on the tree and return its associated value.
	 */
	public ArrayList<TValue> search(TKey key) {
		BTreeLeafNode<TKey,TValue> leaf = this.findLeafNodeShouldContainKey(key);
		
		int index = leaf.search(key);
		return (index == -1) ? null : leaf.getValueList(index);
	}

    public List<TValue> searchRange(TKey leftKey, TKey rightKey) {
        assert leftKey.compareTo(rightKey)<=0 : "leftKey provided is greater than the right key";
        BTreeLeafNode<TKey,TValue> leafLeft=this.findLeafNodeShouldContainKey(leftKey);
        List<TValue> values=leafLeft.searchRange(leftKey, rightKey);
        return values;
    }

	/**
	 * Delete a key and its associated value from the tree. TODO Fix.might have a bug.
	 */
	public void delete(TKey key) {
		BTreeLeafNode<TKey,TValue> leaf = this.findLeafNodeShouldContainKey(key);
		
		if (leaf.delete(key) && leaf.isUnderflow()) {
			BTreeNode<TKey> n = leaf.dealUnderflow();
			if (n != null)
				this.root = n; 
		}
	}
	
	/**
	 * Search the leaf node which should contain the specified key
	 */
	@SuppressWarnings("unchecked")
	private BTreeLeafNode<TKey,TValue> findLeafNodeShouldContainKey(TKey key) {
		BTreeNode<TKey> node = this.root;
		while (node.getNodeType() == TreeNodeType.InnerNode) {
			node = ((BTreeInnerNode<TKey>)node).getChild( node.search(key) );
		}
		
		return (BTreeLeafNode<TKey,TValue>)node;
	}

	/*  method to keep tree template intact, while just removing the tree data payload
	 */
	public void clearPayload() {
		templateMode=true;
		Queue<BTreeNode<TKey>> q=new LinkedList<BTreeNode<TKey>>();
		q.add(this.root);
		while (!q.isEmpty()) {
			BTreeNode<TKey> curr=q.remove();
			if (curr.getNodeType().equals(TreeNodeType.LeafNode)) {
				((BTreeLeafNode) curr).clearNode();

			} else {
				q.addAll(((BTreeInnerNode) curr).children);
			}
		}
	}


	public void printBtree() {
		Queue<BTreeNode<TKey>> q=new LinkedList<BTreeNode<TKey>>();
	//	int height = 0;
	//	int numberOfLeaves = 0;
	//	List<TKey> list = new LinkedList<TKey>();
		q.add(root);
		while (!q.isEmpty()) {
		//	++height;
			Queue<BTreeNode<TKey>> qInner=new LinkedList<BTreeNode<TKey>>();
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
			q=qInner;
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
}
