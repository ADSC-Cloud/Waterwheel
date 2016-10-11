package indexingTopology.util;

import indexingTopology.Config.Config;
import indexingTopology.exception.UnsupportedGenericException;

import java.io.*;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

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

	protected BTreeNode(int order, BytesCounter counter) {
		this.keyCount = 0;
		ORDER = order;
		this.parentNode = null;
		this.leftSibling = null;
		this.rightSibling = null;
		this.counter=counter;
		this.counter.countNewNode();
		this.lock = new ReentrantReadWriteLock();
		this.wLock = lock.writeLock();
		this.rLock = lock.readLock();

	}


	public int getKeyCount() {
//		return this.keyCount;
		int count = 0;
//		acquireReadLock();
		try {
			count = this.keys.size();
		} finally {
//			releaseReadLock();
		}
//		return this.keys.size();  //change to keys.size();
		return count;
	}

	@SuppressWarnings("unchecked")
	public TKey getKey(int index) {
		rLock.lock();
		TKey key;
		try {
			key = this.keys.get(index);
		} finally {
			rLock.unlock();
		}
		return key;
	}

	public void setKey(int index, TKey key) throws UnsupportedGenericException {
//		acquireWriteLock();
		try {
			if (index < this.keys.size())
				this.keys.set(index, key);
			else if (index == this.keys.size()) {
				this.counter.countKeyAddition(UtilGenerics.sizeOf(key.getClass()));
				this.keys.add(index, key);
				keyCount += 1;
			} else {
				throw new ArrayIndexOutOfBoundsException("index is out of bounds");
			}
		} finally {
//			releaseWriteLock();
		}
	}

	public BTreeNode<TKey> getParent() {
//		acquireReadLock();
		BTreeNode<TKey> parent;
		try {
//			return this.parentNode;
			parent = this.parentNode;
		} finally {
//			releaseReadLock();
		}
		return parent;
	}

	public void setParent(BTreeNode<TKey> parent) {
//		acquireWriteLock();
		try {
			this.parentNode = parent;
		} finally {
//			releaseWriteLock();
		}
	}

	public abstract TreeNodeType getNodeType();


	/**
	 * Search a key on current node, if found the key then return its position,
	 * otherwise return -1 for a leaf node,
	 * return the child node index which should contain the key for a internal node.
	 */
	public abstract int search(TKey key);

	public abstract Collection<BTreeNode<TKey>> recursiveSerialize(ByteBuffer allocatedBuffer);

	/* The codes below are used to support insertion operation */

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
//		acquireWriteLock();
		TKey upKey;
		BTreeNode<TKey> newRNode;
		try {
			int midIndex = this.getKeyCount() / 2;
//			TKey upKey = this.getKey(midIndex);
			upKey = this.getKey(midIndex);
//			BTreeNode<TKey> newRNode = this.split();
//			System.out.println("Overflow " + this.keys);
			newRNode = this.split();
//			System.out.println("Rnode " + newRNode.keys);
//			if (this.getParent() != null) {
//				System.out.println("parent" + this.getParent().keys);
//			}
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
				newRNode.setRightSibling(this.getRightSibling());
			}
			this.setRightSibling(newRNode);
		} finally {
//			releaseWriteLock();
		}
		// push up a key to parent internal node
//		synchronized (this.getParent()) {
		if (this.getParent() == null) {
			System.out.println("parent is null");
		}
		return this.getParent().pushUpKey(upKey, this, newRNode);
//		}

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
		acquireReadLock();
		try {
			if (this.leftSibling != null && this.leftSibling.getParent() == this.getParent())
				leftSibling = this.leftSibling;
//				return this.leftSibling;
		} finally {
			releaseReadLock();
		}
		return leftSibling;
	}

	public void setLeftSibling(BTreeNode<TKey> sibling) {
		this.leftSibling = sibling;
	}

	public BTreeNode<TKey> getRightSibling() {
		BTreeNode rightSibling = null;
//		acquireReadLock();
		try {
			if (this.rightSibling != null && this.rightSibling.getParent() == this.getParent()) {
				rightSibling = this.rightSibling;
			}
//				return this.rightSibling;

		} finally {
//			releaseReadLock();
		}
		return rightSibling;
	}

	public void setRightSibling(BTreeNode<TKey> sibling) {
		this.rightSibling = sibling;
	}

	public BTreeNode<TKey> dealUnderflow() {
//		acquireWriteLock();
		BTreeNode node = null;
		if (this.getParent() == null)
//			return null;
			node = null;

		// try to borrow a key from sibling
		BTreeNode<TKey> leftSibling = this.getLeftSibling();
		if (leftSibling != null && leftSibling.canLendAKey()) {
			this.getParent().processChildrenTransfer(this, leftSibling, leftSibling.getKeyCount() - 1);
//			return null;
		}

		BTreeNode<TKey> rightSibling = this.getRightSibling();
		if (rightSibling != null && rightSibling.canLendAKey()) {
			this.getParent().processChildrenTransfer(this, rightSibling, 0);
//			return null;
		}

		// Can not borrow a key from any sibling, then do fusion with sibling
		if (leftSibling != null) {
//			return this.getParent().processChildrenFusion(leftSibling, this);
			node = this.getParent().processChildrenFusion(leftSibling, this);
		}
		else {
//			return this.getParent().processChildrenFusion(this, rightSibling);
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
		double threshold = this.ORDER * Config.TEMPLATE_OVERFLOW_PERCENTAGE;
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
//		System.out.println("r+ on " + this.hashCode()%10 + " by thread " + Thread.currentThread().getId());
	}

	public void releaseReadLock() {
		rLock.unlock();
//		System.out.println("r- on " + this.hashCode()%10 + " by thread " + Thread.currentThread().getId());
	}

	public void acquireWriteLock() {
		wLock.lock();
//		System.out.println("w+ on " + this.hashCode()%10 + " by thread " + Thread.currentThread().getId());
	}

	public void releaseWriteLock() {
		wLock.unlock();
//		System.out.println("w- on " + this.hashCode()%10 + " by thread " + Thread.currentThread().getId());
	}

	public Lock getwLock() {
		return wLock;
	}

	public Lock getrLock() {
		return rLock;
	}
}