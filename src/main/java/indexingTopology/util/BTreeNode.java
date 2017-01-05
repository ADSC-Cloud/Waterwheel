package indexingTopology.util;

import indexingTopology.config.TopologyConfig;
import indexingTopology.exception.UnsupportedGenericException;

import java.io.*;
import java.util.ArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.*;

enum TreeNodeType {
	InnerNode,
	LeafNode
}

public abstract class BTreeNode<TKey extends Comparable<TKey>> implements Serializable{
	protected final int ORDER;
	//   protected final BytesCounter counter;
	protected BytesCounter counter;
	protected ArrayList<TKey> keys;
	protected int keyCount;
	protected BTreeNode<TKey> parentNode;
	protected BTreeNode<TKey> leftSibling;
	protected BTreeNode<TKey> rightSibling;
	protected final ReentrantReadWriteLock lock;
	protected final Lock wLock;
	protected final Lock rLock;

	static AtomicLong idGenerator = new AtomicLong(0);
	long id;

	public long getId() {
		return id;
	}

	protected BTreeNode(int order, BytesCounter counter) {
		this.keyCount = 0;
		ORDER = order;
		this.parentNode = null;
		this.leftSibling = null;
		this.rightSibling = null;
		this.counter=counter;
		if (this instanceof BTreeInnerNode) {
			this.counter.countNewNode();
		}
		this.lock = new ReentrantReadWriteLock();

		this.wLock = lock.writeLock();
		this.rLock = lock.readLock();
		id = idGenerator.getAndIncrement();

	}

	public abstract boolean validateParentReference();

	public abstract boolean validateNoDuplicatedChildReference();

	public abstract boolean validateAllLockReleased();

	public abstract int getDepth();

	public int getKeyCount() {

		return keys.size();
	}

	@SuppressWarnings("unchecked")
	public TKey getKey(int index) {
		TKey key;
		key = this.keys.get(index);
		return key;
	}

	public void setKey(int index, TKey key) throws UnsupportedGenericException {

		if (index < this.keys.size())
			this.keys.set(index, key);
		else if (index == this.keys.size()) {
			if (this instanceof BTreeInnerNode) {
				this.counter.countKeyAdditionOfTemplate(UtilGenerics.sizeOf(key.getClass()));
			}
			this.keys.add(index, key);
			keyCount += 1;
		} else {
			throw new ArrayIndexOutOfBoundsException("index is out of bounds");
		}

	}

	public BTreeNode<TKey> getParent() {
		BTreeNode<TKey> parent;

		parent = this.parentNode;

		return parent;
	}

	public void setParent(BTreeNode<TKey> parent) {
		this.parentNode = parent;
	}

	public abstract TreeNodeType getNodeType();


	/**
	 * Search a key on current node, if found the key then return its position,
	 * otherwise return -1 for a leaf node,
	 * return the child node index which should contain the key for a internal node.
	 */
	public abstract int search(TKey key);

	public boolean isOverflow() {
		return this.getKeyCount() > this.ORDER;
	}

	public boolean isSafe() {
		return this.getKeyCount() < this.ORDER;
	}

	public boolean willOverflowOnInsert(TKey key) {
		for (TKey k : this.keys) {
			if (k.compareTo(key)==0)
				return false;
		}

		return this.getKeyCount() == this.ORDER;
	}

	public BTreeNode<TKey> dealOverflow() {
		TKey upKey;
		BTreeNode<TKey> newRNode;
		Lock parentLock = null;

		int midIndex = this.getKeyCount() / 2;

		upKey = this.getKey(midIndex);

		newRNode = this.split();

		if (this.getParent() == null) {
			this.setParent(new BTreeInnerNode<TKey>(this.ORDER, this.counter));

			counter.increaseHeightCount();
		}

		newRNode.setParent(this.getParent());

		// maintain links of sibling nodes
		newRNode.setLeftSibling(this);
		newRNode.setRightSibling(this.rightSibling);
		if (this.getRightSibling() != null) {
			this.getRightSibling().setLeftSibling(newRNode);
		}

		this.setRightSibling(newRNode);

		// push up a key to parent internal node
		BTreeNode<TKey> ret = this.getParent().pushUpKey(upKey, this, newRNode);

		return ret;

	}

	protected abstract BTreeNode<TKey> split();

	protected abstract BTreeNode<TKey> pushUpKey(TKey key, BTreeNode<TKey> leftChild, BTreeNode<TKey> rightNode);


	/* The codes below are used to support deletion operation */

	public boolean isUnderflow() {
		return this.getKeyCount() < ((this.ORDER+1) / 2);
	}

	public boolean canLendAKey() {
		return this.getKeyCount() > ((this.ORDER+1) / 2);
	}

	public BTreeNode<TKey> getLeftSibling() {
		BTreeNode leftSibling = null;

		if (this.leftSibling != null && this.leftSibling.getParent() == this.getParent())
			leftSibling = this.leftSibling;

		return leftSibling;
	}

	public ArrayList<TKey> getKeys() {
		return new ArrayList<TKey>(keys);
	}


	public void setLeftSibling(BTreeNode<TKey> sibling) {
		this.leftSibling = sibling;
	}

	public BTreeNode<TKey> getRightSibling() {
		BTreeNode rightSibling = null;

		if (this.rightSibling != null && this.rightSibling.getParent() == this.getParent()) {
			rightSibling = this.rightSibling;
		}

		return rightSibling;
	}

	public void setRightSibling(BTreeNode<TKey> sibling) {
		this.rightSibling = sibling;
	}

	public BTreeNode<TKey> dealUnderflow() {

		BTreeNode node = null;

		if (this.getParent() == null)
			node = null;
			// try to borrow a key from sibling
			BTreeNode<TKey> leftSibling = this.getLeftSibling();
			if (leftSibling != null && leftSibling.canLendAKey()) {
				this.getParent().processChildrenTransfer(this, leftSibling, leftSibling.getKeyCount() - 1);
			}

			BTreeNode<TKey> rightSibling = this.getRightSibling();
			if (rightSibling != null && rightSibling.canLendAKey()) {
				this.getParent().processChildrenTransfer(this, rightSibling, 0);
			}

			// Can not borrow a key from any sibling, then do fusion with sibling
			if (leftSibling != null) {
				node = this.getParent().processChildrenFusion(leftSibling, this);
			} else {
				node = this.getParent().processChildrenFusion(this, rightSibling);
			}

		return node;
	}

	protected abstract void processChildrenTransfer(BTreeNode<TKey> borrower, BTreeNode<TKey> lender, int borrowIndex);

	protected abstract BTreeNode<TKey> processChildrenFusion(BTreeNode<TKey> leftChild, BTreeNode<TKey> rightChild);

	protected abstract void fusionWithSibling(TKey sinkKey, BTreeNode<TKey> rightSibling);

	protected abstract TKey transferFromSibling(TKey sinkKey, BTreeNode<TKey> sibling, int borrowIndex);

	public void print() {
		for (TKey k : keys)
			System.out.print(k+" ");
		System.out.println();

	}

	public boolean isOverflowIntemplate() {
		double threshold = this.ORDER * TopologyConfig.TEMPLATE_OVERFLOW_PERCENTAGE;
		return ((double) this.getKeyCount() > threshold);
	}

	public abstract Object clone(BTreeNode oldNode) throws CloneNotSupportedException;


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


	public void acquireReadLock() {
		rLock.lock();
//		System.out.println("The keys are : " + keys);
//		System.out.println("The number of locks is " + lock.getReadLockCount());
//		System.out.println("r+ on " + this.getId() + " by thread " + Thread.currentThread().getId());
	}

	public void releaseReadLock() {
		rLock.unlock();
//		System.out.println("r- on " + this.getId() + " by thread " + Thread.currentThread().getId());
	}

	public void acquireWriteLock() {
		wLock.lock();
//		System.out.println("w+ on " + this.getId() + " by thread " + Thread.currentThread().getId());
	}

	public void releaseWriteLock() {
		wLock.unlock();
//		System.out.println("w- on " + this.getId() + " by thread " + Thread.currentThread().getId());
	}

	public Lock getwLock() {
		return wLock;
	}

	public Lock getrLock() {
		return rLock;
	}

	class MyWriteLock implements Lock {

		ReentrantReadWriteLock.WriteLock lock;
		protected MyWriteLock(ReentrantReadWriteLock.WriteLock lock) {
			this.lock = lock;
		}

		public void lock() {
			lock.lock();
			writeLockThreadId = Thread.currentThread().getId();
		}

		public void lockInterruptibly() throws InterruptedException {

		}

		public boolean tryLock() {
			return false;
		}

		public boolean tryLock(long time, TimeUnit unit) throws InterruptedException {
			return false;
		}

		public void unlock() {
			writeLockThreadId = -1;
			lock.unlock();
		}

		public Condition newCondition() {
			return null;
		}
	}

	class MyReadLock implements Lock {

		ReentrantReadWriteLock.ReadLock lock;
		protected MyReadLock(ReentrantReadWriteLock.ReadLock lock) {
			this.lock = lock;
		}

		public void lock() {
			lock.lock();
			readLockThreadId = Thread.currentThread().getId();
//			System.out.println(String.format("readLock is updated to %d by thread %d", readLockThreadId, Thread.currentThread().getId()));
		}

		public void lockInterruptibly() throws InterruptedException {

		}

		public boolean tryLock() {
			return false;
		}

		public boolean tryLock(long time, TimeUnit unit) throws InterruptedException {
			return false;
		}

		public void unlock() {
			readLockThreadId = -1;
			lock.unlock();
		}

		public Condition newCondition() {
			return null;
		}
	}

	long readLockThreadId;
	long writeLockThreadId;

	public void checkIfCurrentHoldAnyLock() {

//
//		final long tid = Thread.currentThread().getId();
//		final boolean condition = tid == readLockThreadId || tid == writeLockThreadId;
//		assert condition: String.format("Thread %d does not get any lock on node %d", Thread.currentThread().getId(), getId());
//
//		if(!condition) {
//			System.out.println("Hello world!");
//		}
	}

	static public class NodeLock {
		Lock lock;
		long nodeId;
		public NodeLock(Lock lock, long nodeId) {
			this.lock = lock;
			this.nodeId = nodeId;
		}

		public void lock() {
			lock.lock();
		}

		public void unlock() {
			lock.unlock();
		}

		public long getNodeId() {
			return nodeId;
		}
	}
}