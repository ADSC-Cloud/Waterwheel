package indexingTopology.util;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.Queue;

/**
 * Created by acelzj on 11/3/16.
 */
public class DeserializationHelper <TKey extends Comparable<TKey>,TValue>{

    public static BTreeLeafNode deserializeLeaf(byte [] b, int BTreeOrder, BytesCounter counter) throws IOException {
        BTreeLeafNode leaf = new BTreeLeafNode(BTreeOrder, counter);
        int len = Integer.SIZE / Byte.SIZE;
        int offset = 0;
        int keyCount = ByteBuffer.wrap(b, offset, len).getInt();
//        System.out.println("Key count" + keyCount);
        offset += len;
        ArrayList<Double> keys = new ArrayList<Double>();
        for (int i = 0; i < keyCount;i++) {
            len = Double.SIZE / Byte.SIZE;
            Double key = ByteBuffer.wrap(b, offset, len).getDouble();
            keys.add(key);
            offset += len;
        }
        leaf.keys = keys;
        leaf.keyCount = keyCount;
        ArrayList<ArrayList<byte[]>> tuples = new ArrayList<ArrayList<byte[]>>();
        for (int i = 0; i < keys.size();i++) {
            len = Integer.SIZE / Byte.SIZE;
            int tupleCount = ByteBuffer.wrap(b, offset, len).getInt();
            tuples.add(new ArrayList<byte[]>());
            offset += len;
//            System.out.println("Tuple Count " + tupleCount);
            for (int j = 0; j < tupleCount; ++j) {
                int lengthOfTuple = ByteBuffer.wrap(b, offset, len).getInt();
//                System.out.println("length " + lengthOfTuple);
                offset += len;
                byte[] tuple = new byte[lengthOfTuple];
                ByteBuffer.wrap(b, offset, lengthOfTuple).get(tuple);
                tuples.get(i).add(tuple);
                offset += lengthOfTuple;
            }
        }
        leaf.tuples = tuples;
        return leaf;
    }

    public BTree deserializeBTree(byte[] serializedTree, int BTreeOrder, BytesCounter counter) {
        BTreeInnerNode root = null;
        if (serializedTree.length > 0) {
            Queue<BTreeNode<TKey>> q = new LinkedList<BTreeNode<TKey>>();
            int len = Integer.SIZE / Byte.SIZE;
            root = new BTreeInnerNode(BTreeOrder, counter);
            int relativeOffset = 0;
            relativeOffset = deserialize(serializedTree, root, relativeOffset);
            q.add(root);
            while (!q.isEmpty()) {
                BTreeInnerNode<TKey> curr = (BTreeInnerNode) q.remove();
                for (int i = 0; i < curr.getKeyCount() + 1; ++i) {
                    BTreeInnerNode node = new BTreeInnerNode(BTreeOrder, counter);
                    relativeOffset = deserialize(serializedTree, node, relativeOffset);
                    if (node.getKeyCount() != 0) {
                        curr.setChild(i, node);
                    }
                    if (curr.offsets.size() == 0) {
                        q.add(node);
                    }
                }
            }
        }
        TimingModule tm = TimingModule.createNew();
        SplitCounterModule sm = SplitCounterModule.createNew();
        BTree bTree = new BTree(BTreeOrder, tm, sm);
        bTree.setRoot(root);
        return bTree;
    }


    private int deserialize(byte [] b, BTreeInnerNode node, int relativeOffset) {
        int len = Integer.SIZE / Byte.SIZE;
        int offset = relativeOffset;
        int keyCount = ByteBuffer.wrap(b, offset, len).getInt();
        offset += len;
        ArrayList<Double> keys = new ArrayList<Double>();
        for (int i = 0; i < keyCount;i++) {
            len = Double.SIZE / Byte.SIZE;
            Double key = ByteBuffer.wrap(b, offset, len).getDouble();
            keys.add(key);
            offset += len;
        }
        node.keys = keys;
        ArrayList<Integer> offsets = new ArrayList<Integer>();
        len = Character.SIZE / Byte.SIZE;
        char haveOffsets = ByteBuffer.wrap(b, offset, len).getChar();
        offset += len;
        if (haveOffsets == 'y') {
            len = Integer.SIZE / Byte.SIZE;
            int numberOfOffset = ByteBuffer.wrap(b, offset, len).getInt();
            offset += len;
            for (int i = 0; i < numberOfOffset; i++) {
                int offsetOfChild = ByteBuffer.wrap(b, offset, len).getInt();
                offsets.add(offsetOfChild);
                offset += len;
            }
        }
        node.offsets = offsets;
        return offset;
    }
}
