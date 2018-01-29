package indexingTopology.index;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Output;
import indexingTopology.compression.Compressor;
import indexingTopology.compression.CompressorFactory;
import indexingTopology.config.TopologyConfig;
import indexingTopology.exception.UnsupportedGenericException;
import indexingTopology.metrics.TimeMetrics;

import java.io.*;
import java.util.*;

/**
 * A B+ tree
 * Since the structures and behaviors between internal node and external node are different,
 * so there are two different classes for each kind of node.
 * @param <TKey> the data type of the key
 */
public class BTree <TKey extends Comparable<TKey>,TValue> implements Serializable {
	private volatile BTreeNode<TKey> root;
	private boolean templateMode;
	private TopologyConfig config;

	public BTree(int order, TopologyConfig config) {
		this.root = new BTreeLeafNode<>(order);
		templateMode = false;
		this.config = config;
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
		BTreeLeafNode<TKey> leaf = null;
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

	public List<byte[]> searchRange(TKey leftKey, TKey rightKey) {
		assert leftKey.compareTo(rightKey) <= 0 : "leftKey provided is greater than the right key";
		List<byte[]> tuples = new ArrayList<>();

		if (!templateMode) {
			BTreeNode tmpRoot = root;
			tmpRoot.acquireReadLock();
			while (tmpRoot != root) {
				tmpRoot.releaseReadLock();
				tmpRoot = root;
				root.acquireReadLock();
			}
			tuples.addAll(searchRangeInBaseline(leftKey, rightKey));
		} else {
			tuples.addAll(searchRangeInTemplate(leftKey, rightKey));
		}

		return tuples;
	}

	public List<byte[]> searchRangeInBaseline(TKey leftKey, TKey rightKey) {
		assert leftKey.compareTo(rightKey) <= 0 : "leftKey provided is greater than the right key";
		List<byte[]> tuples = new ArrayList<>();
		BTreeNode currentNode = root;
		if (currentNode.getNodeType() == TreeNodeType.LeafNode) {
			tuples.addAll(((BTreeLeafNode) currentNode).getTuplesWithinKeyRange(leftKey, rightKey));
			currentNode.releaseReadLock();
		} else {
			List<BTreeNode> readLockNodes = new ArrayList<>();
			readLockNodes.add(currentNode);

			tuples.addAll(searchTuplesFromCurrentNode(currentNode, leftKey, rightKey, readLockNodes));

			for (BTreeNode ancestor : readLockNodes) {
				ancestor.releaseReadLock();
			}
		}
		return tuples;
	}

	private List<byte[]> searchTuplesFromCurrentNode(BTreeNode currentNode, TKey leftKey, TKey rightKey, List<BTreeNode> readLockNodes) {
		List<byte[]> tuples = new ArrayList<>();
		if (currentNode.getNodeType() == TreeNodeType.LeafNode) {
			return ((BTreeLeafNode) currentNode).getTuplesWithinKeyRange(leftKey, rightKey);
		} else {
			int minIndex = currentNode.search(leftKey);
			int maxIndex = currentNode.search(rightKey);


			BTreeNode node;
			if (minIndex == maxIndex) {
				node = ((BTreeInnerNode) currentNode).getChild(minIndex);

				node.acquireReadLock();
				currentNode.releaseReadLock();

				readLockNodes.remove(currentNode);
				readLockNodes.add(node);
				tuples.addAll(searchTuplesFromCurrentNode(node, leftKey, rightKey, readLockNodes));
			} else {
				for (int i = minIndex; i <= maxIndex; ++i) {
					node = ((BTreeInnerNode) currentNode).getChild(i);
					node.acquireReadLock();
					readLockNodes.add(node);

					tuples.addAll(searchTuplesFromCurrentNode(node, leftKey, rightKey, readLockNodes));
				}
			}
		}
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
			tuples.addAll(currLeaf.getTuplesWithinKeyRange(leftKey, rightKey));
			currLeaf.releaseReadLock();
			currLeaf = (BTreeLeafNode) currLeaf.rightSibling;
		}
		return tuples;
	}



	/**
	 * Search the leaf node which should contain the specified key
	 */
	@SuppressWarnings("unchecked")
	private BTreeLeafNode<TKey> findLeafNodeShouldContainKeyInTemplate(TKey key) {
		BTreeNode currentNode = this.root;
		while (currentNode.getNodeType() == TreeNodeType.InnerNode) {
			final BTreeNode<TKey> node = ((BTreeInnerNode<TKey>) currentNode).getChild(currentNode.search(key));
			currentNode = node;
		}
		return (BTreeLeafNode<TKey>) currentNode;
	}


	/**
	 * Protocol 2 for the updater to find the leaf node that should contain key
	 * @param key
	 * @return the leaf node that contains the key
	 */
	private BTreeLeafNode<TKey> findLeafNodeShouldContainKeyInUpdaterWithProtocolTwo(TKey key) {
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
			BTreeNode<TKey> node = ((BTreeInnerNode<TKey>) currentNode).getChildShouldContainKey(key);
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
		return (BTreeLeafNode<TKey>) currentNode;
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
			BTreeNode<TKey> node = ((BTreeInnerNode<TKey>) currentNode).getChildShouldContainKey(key);
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
			assert curr.lock.getReadLockCount() == 0;
			if (curr.getNodeType().equals(TreeNodeType.LeafNode)) {
				((BTreeInnerNode) curr.getParent()).offsets.clear();
				((BTreeLeafNode) curr).clearNode();

			} else {
				q.addAll(((BTreeInnerNode<TKey>) curr).children);
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
					qInner.addAll(((BTreeInnerNode<TKey>) curr).children);
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


	public BTree getTemplate() {
		BTree bTree = null;
//		try {
//			bTree = (BTree) super.getTemplate();
		bTree = new BTree(config.BTREE_ORDER, config);

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
//		} catch (CloneNotSupportedException e) {
//			e.printStackTrace();
//		}
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

	public byte[] serializeLeavesWithCompressor(Compressor compressor) {
		BTreeLeafNode leaf = getLeftMostLeaf();
		int offset;
		Output output = new Output(60000000, 500000000);
		Kryo kryo = new Kryo();
		kryo.register(BTreeLeafNode.class, new KryoLeafNodeSerializer(config));
		int count = 0;

		if (this.root == leaf) {
			this.root = new BTreeInnerNode(config.BTREE_ORDER);

			((BTreeInnerNode) root).setChild(0, leaf);
		}


		while (leaf != null) {
			offset = output.position();

//			TimeMetrics metrics = new TimeMetrics();

//			metrics.startEvent("serialize");
			Output leafOutput = new Output(650000, 500000000);
			kryo.writeObject(leafOutput, leaf);
//			metrics.endEvent("serialize");

			byte[] bytesToWrite = leafOutput.toBytes();

			byte[] compressed = null;
			try {
//				metrics.startEvent("compress");
				compressed = compressor.compress(bytesToWrite);
//				metrics.endEvent("compress");

//				System.out.println("Ratio: " + (double)compressed.length / bytesToWrite.length);
//				System.out.println(String.format("%.4f KB after compression", compressed.length / 1024.0));
//				System.out.println(metrics);
//				System.out.println("----------");

			} catch (IOException e) {
				e.printStackTrace();
			}



			leafOutput.close();
			if (!config.ChunkOrientedCaching) {
				output.writeInt(compressed.length);
			}
			output.write(compressed);

			((BTreeInnerNode)leaf.getParent()).putOffset(offset);

			leaf = (BTreeLeafNode) leaf.rightSibling;
		}

		byte[] returnBytes = output.toBytes();
		output.close();
		return returnBytes;
	}

	public byte[] serializeLeaves() {
		BTreeLeafNode leaf = getLeftMostLeaf();
		int offset;
		Output output = new Output(60000000, 500000000);
		Kryo kryo = new Kryo();
		kryo.register(BTreeLeafNode.class, new KryoLeafNodeSerializer(config));
		int count = 0;

		if (this.root == leaf) {
			this.root = new BTreeInnerNode(config.BTREE_ORDER);

			((BTreeInnerNode) root).setChild(0, leaf);
		}


		while (leaf != null) {
			offset = output.position();

			TimeMetrics metrics = new TimeMetrics();

			metrics.startEvent("serialize");
			Output leafOutput = new Output(650000, 500000000);
			kryo.writeObject(leafOutput, leaf);
			metrics.endEvent("serialize");

			byte[] bytesToWrite = leafOutput.toBytes();


			leafOutput.close();
			if (!config.ChunkOrientedCaching) {
				output.writeInt(bytesToWrite.length);
			}
			output.write(bytesToWrite);

			((BTreeInnerNode)leaf.getParent()).putOffset(offset);

			leaf = (BTreeLeafNode) leaf.rightSibling;
		}

		byte[] returnBytes = output.toBytes();
		output.close();
		return returnBytes;
	}

	private BTreeNode<TKey> findParentOfLeafNodeShouldContainKey(TKey key) {


//		if (root.getKeyCount() == 0 && ((BTreeInnerNode) root).children.size() == 0) {
//			return root;
//		}


		BTreeInnerNode<TKey> currentNode = (BTreeInnerNode) this.root;
		while (currentNode.children.size() > 0) {
			BTreeNode<TKey> node = ((BTreeInnerNode<TKey>) currentNode).getChildShouldContainKey(key);
			currentNode = (BTreeInnerNode<TKey>) node;
		}

		return currentNode;
	}

	@SuppressWarnings("unchecked")
	public List<Integer> getOffsetsOfLeafNodesShouldContainKeys(TKey leftKey, TKey rightKey) {
		BTreeNode leftNode = findParentOfLeafNodeShouldContainKey(leftKey);
		BTreeNode rightNode = findParentOfLeafNodeShouldContainKey(rightKey);


		List<Integer> offsets = new ArrayList<Integer>();
		BTreeInnerNode<TKey> currentNode = (BTreeInnerNode) leftNode;

		while (currentNode != rightNode) {
			offsets.addAll(currentNode.offsets);
			currentNode = (BTreeInnerNode) currentNode.rightSibling;
		}

		offsets.addAll(((BTreeInnerNode) rightNode).offsets);
		return offsets;
	}

	public double getSkewnessFactor() {
		BTreeLeafNode leaf = getLeftMostLeaf();

		long maxNumberOfTuples = Long.MIN_VALUE;
		long sum = 0;
		long numberOfLeaves = 0;

		while (leaf != null) {
//			long numberOfTuples = leaf.getKeyCount();
			long numberOfTuples = leaf.getAtomicKeyCount();
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

