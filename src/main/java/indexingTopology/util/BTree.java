package indexingTopology.util;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Output;
import indexingTopology.exception.UnsupportedGenericException;

import java.io.*;
import java.util.*;

/**
 * A B+ tree
 * Since the structures and behaviors between internal node and external node are different, 
 * so there are two different classes for each kind of node.
 * @param <TKey> the data type of the key
 */
public class BTree <TKey extends Comparable<TKey>,TValue> implements Serializable, Cloneable{
	private volatile BTreeNode<TKey> root;
	private boolean templateMode;

	public BTree(int order) {
		this.root = new BTreeLeafNode(order);
		templateMode = false;
	}

	public BTreeNode getRoot() {
		return root;
	}

	public void setRoot(BTreeNode root) {
		this.root = root;
	}

	/**
	 * insert the key and value to the B+ tree
	 * based on the mode of the tree, the function will choose the corresponding protocol
	 * @param key the index value
	 * @param serializedTuple  the tuple which has been serialized
	 * @throws UnsupportedGenericException
	 */

	public void insert(TKey key, byte[] serializedTuple) throws UnsupportedGenericException {
		BTreeLeafNode leaf = null;
		if (templateMode) {
			leaf = findLeafNodeShouldContainKeyInTemplate(key);
			leaf.acquireWriteLock();
			leaf.insertKeyTuples(key, serializedTuple, templateMode);
			leaf.releaseWriteLock();
		} else {
			leaf = findLeafNodeShouldContainKeyInUpdaterWithProtocolTwo(key);
			ArrayList<BTreeNode> ancestors = new ArrayList<BTreeNode>();
			//if the root is null, it means that we have to use protocol 1 instead of protocol 2.
			if (leaf == null) {
				leaf = findLeafNodeShouldContainKeyInUpdaterWithProtocolOne(key, ancestors);
			}
			BTreeNode root = leaf.insertKeyTuples(key, serializedTuple, templateMode);
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
	 * search operation for the reader
	 * @param leftKey, rightKey
	 * @return tuples of the corresponding key.
	 */

	/*
	public List<byte[]> searchRange(TKey leftKey, TKey rightKey) {
		assert leftKey.compareTo(rightKey) <= 0 : "leftKey provided is greater than the right key";
		List<byte[]> tuples = null;
		BTreeLeafNode leafLeft;
		BTreeLeafNode leafRight;
		if (!templateMode) {
			leafLeft = this.findLeafNodeShouldContainKeyInReader(leftKey);
		} else {
			leafLeft = this.findLeafNodeShouldContainKeyInTemplate(leftKey);
			leafLeft.acquireReadLock();
		}
        tuples = leafLeft.search(leftKey, rightKey);
		return tuples;
	}
	*/

    public List<byte[]> searchRange(TKey leftKey, TKey rightKey) {
        assert leftKey.compareTo(rightKey) <= 0 : "leftKey provided is greater than the right key";
        List<byte[]> tuples = new ArrayList<>();
        BTreeLeafNode leafLeft;
        BTreeLeafNode leafRight;
        BTreeNode tmpNode;
        if (!templateMode) {
            tmpNode = root;
            tmpNode.acquireReadLock();
            tuples.addAll(searchRangeInTemplate(leftKey, rightKey));
            tmpNode.releaseReadLock();
//            System.out.println(tmpNode == root);
        } else {
            tuples.addAll(searchRangeInTemplate(leftKey, rightKey));
        }
//        tuples = leafLeft.search(leftKey, rightKey);
        return tuples;
    }



    public List<byte[]> searchRangeInTemplate(TKey leftKey, TKey rightKey) {
        assert leftKey.compareTo(rightKey) <= 0 : "leftKey provided is greater than the right key";
        List<byte[]> tuples = new ArrayList<>();
        BTreeLeafNode leafLeft;
        BTreeLeafNode leafRight;
        leafLeft = this.findLeafNodeShouldContainKeyInTemplate(leftKey);
        leafRight = this.findLeafNodeShouldContainKeyInTemplate(rightKey);
        BTreeLeafNode currLeaf = leafLeft;
        while (currLeaf != leafRight.rightSibling) {
            currLeaf.acquireReadLock();
            tuples.addAll(currLeaf.getTuples(leftKey, rightKey));
            currLeaf.releaseReadLock();
            currLeaf = (BTreeLeafNode) currLeaf.rightSibling;
        }
        return tuples;
    }



	/**
	 * Search the leaf node which should contain the specified key
	 */
	@SuppressWarnings("unchecked")
	private BTreeLeafNode findLeafNodeShouldContainKeyInTemplate(TKey key) {
		BTreeNode currentNode = this.root;
		while (currentNode.getNodeType() == TreeNodeType.InnerNode) {
			BTreeNode<TKey> node = ((BTreeInnerNode<TKey>) currentNode).getChild(currentNode.search(key));
			currentNode = node;
		}
		return (BTreeLeafNode) currentNode;
	}

	/**
	 * Protocol for the reader to find the leaf node that should contain key
	 * @param key
	 * @return the leaf node should contain the key
	 */
	private BTreeLeafNode findLeafNodeShouldContainKeyInReader(TKey key) {

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
        return (BTreeLeafNode) currentNode;

	}

	/**
	 * Protocol 2 for the updater to find the leaf node that should contain key
	 * @param key
	 * @return the leaf node that contains the key
	 */
	private BTreeLeafNode findLeafNodeShouldContainKeyInUpdaterWithProtocolTwo(TKey key) {
        BTreeNode tmpRoot = root;
        root.acquireReadLock();
        while (tmpRoot != root) {
            tmpRoot.releaseReadLock();
            tmpRoot = root;
            root.acquireReadLock();
        }

        if (root instanceof BTreeLeafNode) {
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
        return (BTreeLeafNode) currentNode;
	}

	/**
	 * Protocol 1 for the updater to find the leaf node that should contain key
	 * @param key
	 * @param ancestorsOfCurrentNode record the ancestor of the node which needs to be split
	 * @return the leaf node
	 */
	private BTreeLeafNode findLeafNodeShouldContainKeyInUpdaterWithProtocolOne(TKey key, List<BTreeNode> ancestorsOfCurrentNode) {
        BTreeNode tmpRoot = root;
        root.acquireWriteLock();
        while (tmpRoot != root) {
            tmpRoot.releaseWriteLock();
            tmpRoot = root;
            root.acquireWriteLock();
        }

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
		q.add(root);

		while (!q.isEmpty()) {
			Queue<BTreeNode<TKey>> qInner = new LinkedList<BTreeNode<TKey>>();
			while (!q.isEmpty()) {
				BTreeNode<TKey> curr = q.remove();

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
		}
	}

    @Override
	public BTree clone(){
		BTree bTree = null;
		try {
			bTree = (BTree) super.clone();
			List<BTreeNode> leafNodes = new ArrayList<>();
			bTree.root = this.root.deepCopy(leafNodes);

			int count = 0;

			BTreeNode preNode = null;
			for (BTreeNode node : leafNodes) {
			    if (count == 0) {
			        preNode = node;
                } else {
			        node.leftSibling = preNode;
			        preNode.rightSibling = node;
			        preNode = node;
                }
                ++count;
            }
		} catch (CloneNotSupportedException e) {
			e.printStackTrace();
		}
		return bTree;
	}

	public BTreeLeafNode getLeftMostLeaf() {
		BTreeNode<TKey> currentNode = this.root;
		while (currentNode.getNodeType() == TreeNodeType.InnerNode) {
			BTreeNode<TKey> node = ((BTreeInnerNode<TKey>) currentNode).getChild(0);
			currentNode = node;
		}
		return (BTreeLeafNode) currentNode;
	}

	public void setTemplateMode() {
		templateMode = true;
	}

	public byte[] serializeLeaves() {
		BTreeLeafNode leaf = getLeftMostLeaf();
		int offset;
		Output output = new Output(65000000, 20000000);
		Kryo kryo = new Kryo();
		kryo.register(BTreeLeafNode.class, new KryoLeafNodeSerializer());
		int count = 0;
		while (leaf != null) {
			offset = output.position();

			Output outputOfLeaf = new Output(65000, 200000000);

            kryo.writeObject(outputOfLeaf, leaf);

			byte[] bytes = outputOfLeaf.toBytes();
			output.writeInt(bytes.length);
			output.write(bytes);

			((BTreeInnerNode)leaf.getParent()).putOffset(offset);

			leaf = (BTreeLeafNode) leaf.rightSibling;
		}
		return output.toBytes();
	}

	public BTreeNode findLeafNodeShouldContainKeyInDeserializedTemplate(TKey key) {
		BTreeInnerNode<TKey> currentNode = (BTreeInnerNode) this.root;
		while (currentNode.children.size() > 0) {
			BTreeNode<TKey> node = ((BTreeInnerNode<TKey>) currentNode).getChildWithSpecificIndex(key);
			currentNode = (BTreeInnerNode) node;
		}
		return (BTreeNode) currentNode;
	}

	@SuppressWarnings("unchecked")
	public List<Integer> getOffsetsOfLeaveNodesShouldContainKeys(BTreeNode mostLeftNode
			, BTreeNode mostRightNode) {
		List<Integer> offsets = new ArrayList<Integer>();
		BTreeInnerNode<TKey> currentNode = (BTreeInnerNode) mostLeftNode;
		while (currentNode != mostRightNode) {
			offsets.addAll(currentNode.offsets);
			currentNode = (BTreeInnerNode) currentNode.rightSibling;
		}
		offsets.addAll(((BTreeInnerNode) mostRightNode).offsets);
		return offsets;
	}

	public double getSkewnessFactor() {
		BTreeLeafNode leaf = getLeftMostLeaf();

		long maxNumberOfTuples = Long.MIN_VALUE;
		long sum = 0;
		long numberOfLeaves = 0;

		while (leaf != null) {
			long numberOfTuples = leaf.getTupleCount();

			if (numberOfTuples > maxNumberOfTuples) {
				maxNumberOfTuples = numberOfTuples;
			}

			sum += numberOfTuples;
			++numberOfLeaves;
			leaf = (BTreeLeafNode) leaf.rightSibling;
		}

		double averageNumberOfTuples = sum / (double) numberOfLeaves;

		return (maxNumberOfTuples - averageNumberOfTuples) / averageNumberOfTuples;
	}

	public int getHeight() {
	    int height = 1;
        BTreeNode<TKey> currentNode = this.root;
        while (currentNode.getNodeType() == TreeNodeType.InnerNode) {
            BTreeNode<TKey> node = ((BTreeInnerNode<TKey>) currentNode).getChild(0);
            currentNode = node;
            ++height;
        }
        return height;
    }

}

