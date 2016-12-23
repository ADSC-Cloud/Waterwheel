package indexingTopology.util;

import indexingTopology.Config.TopologyConfig;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.Queue;

/**
 * Created by acelzj on 12/4/16.
 */
public class SerializationHelper {

    public static byte[] serializeTree(BTree bTree) {
        ByteBuffer b = ByteBuffer.allocate(TopologyConfig.TEMPLATE_SIZE);

        Queue<BTreeNode> q = new LinkedList<BTreeNode>();
        q.add(bTree.getRoot());

        while (!q.isEmpty()) {
            BTreeInnerNode curr = (BTreeInnerNode) q.remove();
            b.put(serializeInnerNode(curr));
            if (curr.children.size() > 0 && curr.getChild(0).getNodeType() == TreeNodeType.InnerNode) {
                q.addAll(curr.children);
            }
        }

        return b.array();
    }

    /**
     * The content of the byte array is as following
     * [key count of the inner node  [key1, key2 ...] ['y' or 'n] [offset of its node] (not necessary)]
     * if this node is in the last but one layer, the content will be 'y' else the content will be 'n'.
     * if the content is 'y' it will have the offset in the chunk of its children
     * @return the serialized inner node in a byte array
     */
    private static byte[] serializeInnerNode(BTreeInnerNode node) {
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        byte [] b = ByteBuffer.allocate(Integer.SIZE / Byte.SIZE).putInt(node.getKeys().size()).array();
        writeToByteArrayOutputStream(bos, b);

        for (int i = 0; i < node.getKeys().size(); i++) {
            b = ByteBuffer.allocate(Double.SIZE / Byte.SIZE).putDouble((Double) node.getKeys().get(i)).array();
            writeToByteArrayOutputStream(bos, b);
        }

        if (node.getOffsets().size() != 0) {
            b = ByteBuffer.allocate(Character.SIZE / Byte.SIZE).putChar('y').array();
            writeToByteArrayOutputStream(bos, b);
            b = ByteBuffer.allocate(Integer.SIZE / Byte.SIZE).putInt(node.getOffsets().size()).array();
            writeToByteArrayOutputStream(bos, b);
            for (int i = 0; i < node.getOffsets().size(); i++) {
                b = ByteBuffer.allocate(Integer.SIZE / Byte.SIZE).putInt((Integer) node.getOffsets().get(i)).array();
                writeToByteArrayOutputStream(bos, b);
            }
        } else {
            b = ByteBuffer.allocate(Character.SIZE / Byte.SIZE).putChar('n').array();
            writeToByteArrayOutputStream(bos, b);
        }
        return bos.toByteArray();
    }

    public static byte[] serializeLeafNode(BTreeLeafNode leaf) {

        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        int totalBytes = leaf.bytesCount + (1 + leaf.tuples.size()) * (Integer.SIZE / Byte.SIZE);
        byte[] b = ByteBuffer.allocate(Integer.SIZE / Byte.SIZE).putInt(totalBytes).array();
        writeToByteArrayOutputStream(bos, b);

        b = ByteBuffer.allocate(Integer.SIZE / Byte.SIZE).putInt(leaf.keys.size()).array();
        writeToByteArrayOutputStream(bos, b);

        for (int i = 0; i < leaf.keys.size(); ++i) {
            b = ByteBuffer.allocate(Double.SIZE / Byte.SIZE).putDouble((Double) leaf.keys.get(i)).array();
            writeToByteArrayOutputStream(bos, b);
        }

        for (int i = 0;i < leaf.keys.size(); i++) {
            b = ByteBuffer.allocate(Integer.SIZE / Byte.SIZE).putInt(((ArrayList<byte []>) leaf.tuples.get(i)).size()).array();
            writeToByteArrayOutputStream(bos, b);
            for (int j = 0; j < ((ArrayList<byte []>) leaf.tuples.get(i)).size(); ++j) {
                b = ByteBuffer.allocate(Integer.SIZE / Byte.SIZE).putInt(((ArrayList<Integer>)leaf.offsets.get(i)).get(j)).array();
                writeToByteArrayOutputStream(bos, b);
                writeToByteArrayOutputStream(bos, ((ArrayList<byte []>) leaf.tuples.get(i)).get(j));
            }
        }
        return bos.toByteArray();
    }

    private static void writeToByteArrayOutputStream(ByteArrayOutputStream bos, byte[] b) {
        try {
            bos.write(b);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

}
