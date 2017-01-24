package indexingTopology.util;

import indexingTopology.config.TopologyConfig;
import indexingTopology.exception.UnsupportedGenericException;

import java.io.*;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.*;

enum TreeNodeType {
	InnerNode,
	LeafNode
}

public abstract class BTreeNode<TKey extends Comparable<TKey>> implements Serializable{
	protected final int ORDER;
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

	protected BTreeNode(int order) {
		this.keyCount = 0;
		ORDER = order;
		this.parentNode = null;
		this.leftSibling = null;
		this.rightSibling = null;
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

	public BTreeNode<TKey> dealOverflow() {
		TKey upKey;
		BTreeNode<TKey> newRNode;
		Lock parentLock = null;

		int midIndex = this.getKeyCount() / 2;

		upKey = this.getKey(midIndex);

		newRNode = this.split();

		if (this.getParent() == null) {
			this.setParent(new BTreeInnerNode<TKey>(this.ORDER));
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

	public void print() {
		for (TKey k : keys)
			System.out.print(k+" ");
		System.out.println();

	}

	public void acquireReadLock() {
		rLock.lock();
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

	public abstract BTreeNode deepCopy(List<BTreeNode> nodes);
}