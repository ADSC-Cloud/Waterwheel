package indexingTopology.util;

import indexingTopology.exception.UnsupportedGenericException;

import java.io.*;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

class BTreeInnerNode<TKey extends Comparable<TKey>> extends BTreeNode<TKey> implements Serializable {
	protected ArrayList<BTreeNode<TKey>> children;


	public BTreeInnerNode(int order, BytesCounter counter) {
		super(order,counter);
		this.keys = new ArrayList<TKey>();
		this.children = new ArrayList<BTreeNode<TKey>>();

	}

	public boolean validateParentReference() {
		for(BTreeNode<TKey> child: children) {
			if(child.getParent().getId() != getId()) {
				System.out.println(String.format("%d's parent reference is wrong!", child.getId()));
				System.out.println(String.format("%d's parent reference is %d, should be %d", child.getId(), child.getParent().getId(), getId()));
				return false;
			}
			if(!child.validateParentReference()) {
				System.out.println(String.format("- %d ->", getId()));
				return false;
			}
		}
		return true;
	}

	public boolean validateNoDuplicatedChildReference() {
		Set<Long> idSet = new HashSet<Long>();
		for(BTreeNode<TKey> child: children) {
			if(idSet.contains(child.getId())) {
				System.out.println(String.format("Duplicated child %d is found on %d", child.getId(), getId()));
				return false;
			}
			idSet.add(child.getId());

			if(!child.validateNoDuplicatedChildReference()) {
				System.out.println(String.format(" -- %d -->", getId()));
				return false;
			}
		}
		return true;
	}

	public boolean validateAllLockReleased() {
//		if(lock.isWriteLocked() || lock.getReadLockCount() > 0) {
//			return false;
//		}
//		for(BTreeNode<TKey> node: children) {
//			if(!node.validateAllLockReleased())
//				return false;
//		}
		return true;
	}

	public int getDepth() {
		int ret = 1;
		for(BTreeNode<TKey> node: children) {
			ret = Math.max(ret, node.getDepth() + 1);
		}
		return ret;
	}

	@SuppressWarnings("unchecked")
	public BTreeNode<TKey> getChild(int index) {
//		acquireReadLock();
		BTreeNode node;
//		try {
			node = this.children.get(index);
//		} finally {
//			releaseReadLock();
//		}
		return node;
	}

	public BTreeNode<TKey> getChildWithSpecificIndex(TKey key) {
//		acquireReadLock();
		BTreeNode node;
//		try {
        int index = search(key);
        node = this.children.get(index);
//		} finally {
//			releaseReadLock();
//		}
		return node;
	}

	public BTreeInnerNode<TKey> getRightMostChild() {
//		acquireReadLock();
		BTreeInnerNode root = this;
//		try {
//			if (root == null) {
//				System.out.println("yes");
//			}
			//	root.print();
			while (root.children.size() > 0 && root.getChild(0).getNodeType() == TreeNodeType.InnerNode) {
				int index = root.children.size();
				root = (BTreeInnerNode) root.getChild(index - 1);
			}
//			if (root == null) {
//				System.out.println("yes");
//			}
//		} finally {
//			releaseReadLock();
//		}
		return (BTreeInnerNode) root.parentNode;
	}

	public BTreeInnerNode<TKey> getRightMostChildTest() {
//		acquireReadLock();
		BTreeInnerNode root = this;
//		try {
//			if (root == null) {
//				System.out.println("yes");
//			}
		//	root.print();
		while (root.getChild(0).getNodeType() == TreeNodeType.InnerNode) {
			int index = root.children.size();
			root = (BTreeInnerNode) root.getChild(index - 1);
		}
//			if (root == null) {
//				System.out.println("yes");
//			}
//		} finally {
//			releaseReadLock();
//		}
		return root;
	}

	public void setChild(int index, BTreeNode<TKey> child) {
//		acquireWriteLock();
//		try {
			if (index < children.size())
				this.children.set(index, child);
//			    this.children.add(index, child);
			else if (index == children.size())
				this.children.add(child);
			else
				throw new ArrayIndexOutOfBoundsException("Out of bounds");

			if (child != null)
				child.setParent(this);
//		} finally {
//			releaseWriteLock();
//		}
	}

	@Override
	public TreeNodeType getNodeType() {
		return TreeNodeType.InnerNode;
	}

	@Override
	public int search(TKey key) {
		int index = 0;
		for (index = 0; index < this.getKeyCount(); ++index) {
			int cmp = this.getKey(index).compareTo(key);
			if (cmp == 0) {
				return index + 1;
//				return index;
			}
			else if (cmp > 0) {
				return index;
			}
		}
		return index;
	}

//	public int search(TKey key) {
//		int low = 0;
//		int high = this.getKeyCount() - 1;
//		while (low <= high) {
//			int mid = (low + high) >> 1;
//			int cmp = this.getKey(mid).compareTo(key);
//			if (cmp == 0) {
//				return (mid + 1);
//			} else if (cmp > 0) {
//				high = mid - 1;
//			} else {
//				low = mid + 1;
//			}
//		}
//		return low;
//	}


	public Collection<BTreeNode<TKey>> recursiveSerialize(ByteBuffer allocatedBuffer) {
		allocatedBuffer.put((byte) 'i');
		allocatedBuffer.putInt(this.getKeyCount());
		for (Object k : this.keys) {
			try {
				UtilGenerics.putIntoByteBuffer(allocatedBuffer,k);
			} catch (UnsupportedGenericException e) {
				e.printStackTrace();
			}
		}

		return this.children;
	}


	/* The codes below are used to support insertion operation */

	private void insertAt(int index, TKey key, BTreeNode<TKey> leftChild, BTreeNode<TKey> rightChild) {
//		acquireWriteLock();
//		try {
			try {
				counter.countKeyAddition(UtilGenerics.sizeOf(key.getClass()));
			} catch (UnsupportedGenericException e) {
				e.printStackTrace();
			}
			this.keys.add(index, key);
			this.children.add(index, leftChild);
			this.setChild(index + 1, rightChild);
			this.keyCount += 1;
//		} finally {
//			releaseWriteLock();
//		}
	}

	/**
	 * When splits a internal node, the middle key is kicked out and be pushed to parent node.
	 */
	@Override
	protected BTreeNode<TKey> split() {
//		System.out.println(String.format("Inner node %d is spilt!", getId()));
//		acquireWriteLock();
		BTreeInnerNode<TKey> newRNode = new BTreeInnerNode<TKey>(this.ORDER, this.counter);
//		try {
//			BTreeInnerNode<TKey> newRNode = new BTreeInnerNode<TKey>(this.ORDER, this.counter);

			int midIndex = this.getKeyCount() / 2;

			for (int i = midIndex + 1; i < this.getKeyCount(); ++i) {
				try {
					newRNode.setKey(i - midIndex - 1, this.getKey(i));
				} catch (UnsupportedGenericException e) {
					e.printStackTrace();
				}
			}

			for (int i = midIndex + 1; i <= this.getKeyCount(); ++i) {
				newRNode.setChild(i - midIndex - 1, this.getChild(i));
				newRNode.getChild(i - midIndex - 1).setParent(newRNode);
			}

			newRNode.keyCount = this.getKeyCount() - midIndex - 1;

			for (int i = this.getKeyCount() - 1; i >= midIndex; i--) {
				this.deleteAt(i);
			}
//		} finally {
//			releaseWriteLock();
//		}
		return newRNode;
	}

	@Override
	protected BTreeNode<TKey> pushUpKey(TKey key, BTreeNode<TKey> leftChild, BTreeNode<TKey> rightNode) {
		// find the target position of the new key
		BTreeNode root = null;
//		acquireWriteLock();
//		try {
//		acquireWriteLock();
			int index = this.search(key);

			// note that the there might be duplicated keys here. So the insertion may not be correct if only locating
			// insertion point by the key.

			// insert the new key
//			this.insertAt(index, key, leftChild, rightNode);



			if(children.size() == 0) {

				keys.add(0, key);
				children.add(leftChild);
				children.add(rightNode);

			} else {
				for(int i = 0; i < children.size(); i++) {
					if(children.get(i) == leftChild) {
						keys.add(i, key);
						children.add(i + 1, rightNode);
					}
				}
			}
			try {
				counter.countKeyAddition(UtilGenerics.sizeOf(key.getClass()));
			} catch (UnsupportedGenericException e) {
				e.printStackTrace();
			}
			keyCount++;



			// check whether current node need to be split
			if (this.isOverflow()) {
//				System.out.println("Keys " + this.keys);
				root = this.dealOverflow();
//				System.out.println("push up" + this.keys);
//				return this.dealOverflow(leaf);
			} else {
				if (this.getParent() == null) {
//					return this;
					root = this;
				} else {
					root = this.getParent();
					while (root.getParent() != null) {
						root = root.getParent();
					}
//					return root;
				}
			}
//		} finally {
//			releaseWriteLock();
//		}
		return root;
	}


	/* The codes below are used to support delete operation */

	private void deleteAt(int index) {
//		acquireWriteLock();
//		try {
			try {
				counter.countKeyRemoval(UtilGenerics.sizeOf(this.keys.get(index).getClass()));
			} catch (UnsupportedGenericException e) {
				e.printStackTrace();
			}
			this.keys.remove(index);
			this.children.remove(index + 1);
			--this.keyCount;
//		} finally {
//			releaseWriteLock();
//		}
	}


	@Override
	protected void processChildrenTransfer(BTreeNode<TKey> borrower, BTreeNode<TKey> lender, int borrowIndex) {
//		acquireWriteLock();
//		try {
			int borrowerChildIndex = 0;
			while (borrowerChildIndex < this.getKeyCount() + 1 && this.getChild(borrowerChildIndex) != borrower)
				++borrowerChildIndex;

			if (borrowIndex == 0) {
				// borrow a key from right sibling
				TKey upKey = borrower.transferFromSibling(this.getKey(borrowerChildIndex), lender, borrowIndex);
				try {
					this.setKey(borrowerChildIndex, upKey);
				} catch (UnsupportedGenericException e) {
					e.printStackTrace();
				}
			} else {
				// borrow a key from left sibling
				TKey upKey = borrower.transferFromSibling(this.getKey(borrowerChildIndex - 1), lender, borrowIndex);
				try {
					this.setKey(borrowerChildIndex - 1, upKey);
				} catch (UnsupportedGenericException e) {
					e.printStackTrace();
				}
			}
//		} finally {
//			releaseWriteLock();
//		}
	}

	@Override
	protected BTreeNode<TKey> processChildrenFusion(BTreeNode<TKey> leftChild, BTreeNode<TKey> rightChild) {
		BTreeNode<TKey> node = null;
//		acquireWriteLock();
//		try {
			int index = 0;
			while (index < this.getKeyCount() && this.getChild(index) != leftChild)
				++index;
			TKey sinkKey = this.getKey(index);

			// merge two children and the sink key into the left child node
			leftChild.fusionWithSibling(sinkKey, rightChild);

			// remove the sink key, keep the left child and abandon the right child
			this.deleteAt(index);

			// check whether need to propagate borrow or fusion to parent
			if (this.isUnderflow()) {
				if (this.getParent() == null) {
					// current node is root, only remove keys or delete the whole root node
					if (this.getKeyCount() == 0) {
						leftChild.setParent(null);
//					return leftChild;
						node = leftChild;
					} else {
//					return null;
						node = null;
					}
				}

//			return this.dealUnderflow();
				node = this.dealUnderflow();
			}
//		} finally {
//			releaseWriteLock();
//		}
//
//		return null;
		return node;
	}


	@Override
	protected void fusionWithSibling(TKey sinkKey, BTreeNode<TKey> rightSibling) {
//		acquireWriteLock();
//		try {
			BTreeInnerNode<TKey> rightSiblingNode = (BTreeInnerNode<TKey>) rightSibling;

			int j = this.getKeyCount();
			try {
				this.setKey(j++, sinkKey);
			} catch (UnsupportedGenericException e) {
				e.printStackTrace();
			}

			for (int i = 0; i < rightSiblingNode.getKeyCount(); ++i) {
				try {
					this.setKey(j + i, rightSiblingNode.getKey(i));
				} catch (UnsupportedGenericException e) {
					e.printStackTrace();
				}
			}
			for (int i = 0; i < rightSiblingNode.getKeyCount() + 1; ++i) {
				this.setChild(j + i, rightSiblingNode.getChild(i));
			}
			this.keyCount += 1 + rightSiblingNode.getKeyCount();

			this.setRightSibling(rightSiblingNode.rightSibling);
			if (rightSiblingNode.rightSibling != null)
				rightSiblingNode.rightSibling.setLeftSibling(this);
//		} finally {
//			releaseWriteLock();
//		}

	}

	@Override
	protected TKey transferFromSibling(TKey sinkKey, BTreeNode<TKey> sibling, int borrowIndex) {

		TKey upKey = null;
//		acquireWriteLock();
//		try {
			BTreeInnerNode<TKey> siblingNode = (BTreeInnerNode<TKey>) sibling;
			if (borrowIndex == 0) {
				// borrow the first key from right sibling, append it to tail
				int index = this.getKeyCount();
				try {
					this.setKey(index, sinkKey);
				} catch (UnsupportedGenericException e) {
					e.printStackTrace();
				}
				this.setChild(index + 1, siblingNode.getChild(borrowIndex));
				this.keyCount += 1;

				upKey = siblingNode.getKey(0);
				siblingNode.deleteAt(borrowIndex);
			} else {
				// borrow the last key from left sibling, insert it to head
				this.insertAt(0, sinkKey, siblingNode.getChild(borrowIndex + 1), this.getChild(0));
				upKey = siblingNode.getKey(borrowIndex);
				siblingNode.deleteAt(borrowIndex);
			}
//		} finally {
//			releaseWriteLock();
//		}
		return upKey;
	}


	public Object clone(BTreeNode oldNode) throws CloneNotSupportedException{
		BTreeInnerNode node = new BTreeInnerNode(ORDER, (BytesCounter) counter.clone());
		node.keyCount = keyCount;
		//	node.counter = (BytesCounter) counter.clone();
		if (parentNode != null) {
			node.parentNode = oldNode;
		}
		if (leftSibling != null) {
			//	node.leftSibling = (BTreeNode) leftSibling.clone(oldNode);
			node.leftSibling = oldNode.leftSibling;
		}
		if (rightSibling != null) {
			//	node.rightSibling = (BTreeNode) rightSibling.clone(oldNode);
			node.rightSibling = oldNode.rightSibling;
		}


		node.keys.addAll(keys);

		for (BTreeNode child : children) {
			BTreeNode newNode = (BTreeNode) child.clone(node);
			node.children.add(newNode);
		}
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

	public void insertKey(TKey key) {
		this.keys.add(key);
		keyCount += 1;
	}
}