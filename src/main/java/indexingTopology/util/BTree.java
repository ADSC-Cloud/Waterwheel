package indexingTopology.util;

import indexingTopology.exception.UnsupportedGenericException;

import java.nio.ByteBuffer;
import java.util.*;

/**
 * A B+ tree
 * Since the structures and behaviors between internal node and external node are different, 
 * so there are two different classes for each kind of node.
 * @param <TKey> the data type of the key
 */
public class BTree<TKey extends Comparable<TKey>,TValue> {
	private BTreeNode<TKey> root;
    private final BytesCounter counter;
	private final TimingModule tm;
	private boolean templateMode;
    private final SplitCounterModule sm;
	
	public BTree(int order, TimingModule tm, SplitCounterModule sm) {
		counter = new BytesCounter();
        this.root = new BTreeLeafNode<TKey,TValue>(order,counter);
        counter.increaseHeightCount();
		templateMode = false;

		assert tm != null : "Timing module cannot be null";
		assert sm != null : "Split counter module cannot be null";
		this.tm = tm;
		this.sm = sm;
	}

	public void setRoot(BTreeNode root) {
		this.root = root;
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
		leaf = this.findLeafNodeShouldContainKey(key);

//		for (int i=0;i<1000;i++) {
//			tm.startTiming(Constants.TIME_LEAF_FIND.str);
//			leaf = this.findLeafNodeShouldContainKey(key);
//			tm.endTiming(Constants.TIME_LEAF_FIND.str);
//		}

		synchronized (leaf) {
			leaf.insertKeyValue(key, value);
		}

//		for (int i=0;i<1000;i++) {
//			tm.startTiming(Constants.TIME_LEAF_INSERTION.str);
//			leaf.insertKeyValue(key,value);
//			tm.endTiming(Constants.TIME_LEAF_INSERTION.str);
//			leaf.deleteKeyValue(key,value);
//		}
        if (templateMode && leaf.isOverflow()) {
			tm.putDuration(Constants.TIME_SPLIT.str, 0);
			sm.addCounter();
		} else if (!leaf.isOverflow()) {
			tm.putDuration(Constants.TIME_SPLIT.str, 0);
		} else {
       // if (templateMode || !leaf.isOverflow()) {
       //     tm.putDuration(Constants.TIME_SPLIT.str, 0);
      //  } else {
            tm.startTiming(Constants.TIME_SPLIT.str);
			BTreeNode<TKey> n = leaf.dealOverflow(sm, leaf);
			if (n != null) {
				this.root = n;
				tm.endTiming(Constants.TIME_SPLIT.str);
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
		q.add(root);
		while (!q.isEmpty()) {
			Queue<BTreeNode<TKey>> qInner=new LinkedList<BTreeNode<TKey>>();
			while (!q.isEmpty()) {
				BTreeNode<TKey> curr = q.remove();
				if (curr.getNodeType().equals(TreeNodeType.InnerNode)) {
					qInner.addAll(((BTreeInnerNode) curr).children);
				}

				for (TKey k : curr.keys)
					System.out.print(k+" ");

				System.out.print(": ");
			}

			System.out.println();
			q=qInner;
		}
	}
}
