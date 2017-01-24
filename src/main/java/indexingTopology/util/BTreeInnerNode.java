package indexingTopology.util;

import indexingTopology.exception.UnsupportedGenericException;
import org.omg.Messaging.SYNC_WITH_TRANSPORT;

import java.io.*;
import java.util.*;

class BTreeInnerNode<TKey extends Comparable<TKey>> extends BTreeNode<TKey> implements Serializable {
	protected ArrayList<BTreeNode<TKey>> children;

	protected ArrayList<Integer> offsets;


	public BTreeInnerNode(int order) {
		super(order);
		this.keys = new ArrayList<TKey>();
		this.children = new ArrayList<BTreeNode<TKey>>();
		this.offsets = new ArrayList<Integer>();
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
		BTreeNode node;
		node = this.children.get(index);
		return node;
	}

	public ArrayList<BTreeNode<TKey>> getChildren() {
		return children;
	}

	public BTreeNode<TKey> getChildWithSpecificIndex(TKey key) {
		BTreeNode node;
        int index = search(key);
        node = this.children.get(index);
		return node;
	}

	public BTreeInnerNode<TKey> getRightMostChild() {
		BTreeInnerNode root = this;
		while (root.children.size() > 0 && root.getChild(0).getNodeType() == TreeNodeType.InnerNode) {
			int index = root.children.size();
			root = (BTreeInnerNode) root.getChild(index - 1);
		}
		return (BTreeInnerNode) root.parentNode;
	}

	public void setChild(int index, BTreeNode<TKey> child) {
		if (index < children.size())
			this.children.set(index, child);
		else if (index == children.size())
			this.children.add(child);
		else
			throw new ArrayIndexOutOfBoundsException("Out of bounds");
		if (child != null)
			child.setParent(this);
	}

	@Override
	public TreeNodeType getNodeType() {
		return TreeNodeType.InnerNode;
	}

	@Override
	public int search(TKey key) {
		int low = 0;
		int high = this.getKeyCount() - 1;
		while (low <= high) {
			int mid = (low + high) >> 1;
			int cmp = this.getKey(mid).compareTo(key);
			if (cmp == 0) {
				return (mid + 1);
			} else if (cmp > 0) {
				high = mid - 1;
			} else {
				low = mid + 1;
			}
		}
		return low;
	}

	/**
	 * When splits a internal node, the middle key is kicked out and be pushed to parent node.
	 */
	@Override
	protected BTreeNode<TKey> split() {
		BTreeInnerNode<TKey> newRNode = new BTreeInnerNode<TKey>(this.ORDER);

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

		return newRNode;
	}

	@Override
	protected BTreeNode<TKey> pushUpKey(TKey key, BTreeNode<TKey> leftChild, BTreeNode<TKey> rightNode) {
		// find the target position of the new key
		BTreeNode root = null;
		// note that the there might be duplicated keys here. So the insertion may not be correct if only locating
		// insertion point by the key.

		// insert the new key
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

		keyCount++;
		// check whether current node need to be split
		if (this.isOverflow()) {
			root = this.dealOverflow();
		} else {
			if (this.getParent() == null) {
				root = this;
			} else {
				root = this.getParent();
				while (root.getParent() != null) {
					root = root.getParent();
				}
			}
		}
		return root;
	}


	public void insertKey(TKey key) {
		this.keys.add(key);
		keyCount += 1;
	}


	public void putOffset(int offset) {
		offsets.add(offset);
	}

	public ArrayList<Integer> getOffsets() {
		return new ArrayList<Integer>(offsets);
	}


	/* The codes below are used to support delete operation */

	private void deleteAt(int index) {
		this.keys.remove(index);
		this.children.remove(index + 1);
		--this.keyCount;
	}

    @Override
	@SuppressWarnings("unchecked")
    public BTreeNode deepCopy(List<BTreeNode> nodes) {
        BTreeInnerNode node = new BTreeInnerNode(ORDER);
        node.keys = (ArrayList) this.keys.clone();

        for (BTreeNode child : children) {
            BTreeNode newNode = child.deepCopy(nodes);
            node.children.add(newNode);
            newNode.parentNode = node;
        }

        return node;
    }
}