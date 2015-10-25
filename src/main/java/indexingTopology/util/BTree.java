package indexingTopology.util;

import indexingTopology.exception.UnsupportedGenericException;

import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;

/**
 * A B+ tree
 * Since the structures and behaviors between internal node and external node are different, 
 * so there are two different classes for each kind of node.
 * @param <TKey> the data type of the key
 */
public class BTree<TKey extends Comparable<TKey>> {
	private BTreeNode<TKey> root;
    private BytesCounter counter;
	
	public BTree(int order) {
		counter=new BytesCounter();
        this.root = new BTreeLeafNode<TKey>(order,counter);
        counter.increaseHeightCount();
	}

    public int getTotalBytes() {
        return counter.getCount();
    }

    public int getBytesEstimateForInsert(TKey key,byte [] value) throws UnsupportedGenericException {
        return counter.getBytesEstimateForInsert(UtilGenerics.sizeOf(key.getClass()), value.length);
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
	 */
	public void insert(TKey key, byte [] value) throws UnsupportedGenericException {
		BTreeLeafNode<TKey> leaf = this.findLeafNodeShouldContainKey(key);
        leaf.insertKey(key, value);

        if (leaf.isOverflow()) {
			BTreeNode<TKey> n = leaf.dealOverflow();
			if (n != null)
				this.root = n;
		}
	}

	/**
	 * TODO what happens if same key different value
	 * Search a key value on the tree and return its associated value.
	 */
	public byte [] search(TKey key) {
		BTreeLeafNode<TKey> leaf = this.findLeafNodeShouldContainKey(key);
		
		int index = leaf.search(key);
		return (index == -1) ? null : leaf.getValue(index);
	}

    public List<byte[]> searchRange(TKey leftKey, TKey rightKey) {
        assert leftKey.compareTo(rightKey)<=0 : "leftKey provided is greater than the right key";
        BTreeLeafNode<TKey> leafLeft=this.findLeafNodeShouldContainKey(leftKey);
        List<byte[]> values=leafLeft.searchRange(leftKey, rightKey);
        return values;
    }

	/**
	 * Delete a key and its associated value from the tree. TODO Fix.might have a bug.
	 */
	public void delete(TKey key) {
		BTreeLeafNode<TKey> leaf = this.findLeafNodeShouldContainKey(key);
		
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
	private BTreeLeafNode<TKey> findLeafNodeShouldContainKey(TKey key) {
		BTreeNode<TKey> node = this.root;
		while (node.getNodeType() == TreeNodeType.InnerNode) {
			node = ((BTreeInnerNode<TKey>)node).getChild( node.search(key) );
		}
		
		return (BTreeLeafNode<TKey>)node;
	}

    public int getTotalHeight() {
        return counter.getHeightCount();
    }
}
