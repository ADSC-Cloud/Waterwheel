package indexingTopology.util;

import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import com.sun.prism.shader.Solid_TextureYV12_AlphaTest_Loader;
import indexingTopology.DataSchema;
import indexingTopology.FileSystemHandler.FileSystemHandler;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.*;

/**
 * Created by acelzj on 11/3/16.
 */
public class DeserializationHelper <TKey extends Comparable<TKey>,TValue>{


//    static List<String> fieldNames = new ArrayList<String>(Arrays.asList("user_id", "id_1", "id_2", "ts_epoch",
//            "date", "time", "latitude", "longitude"));
//    static List<Class> valueTypes = new ArrayList<Class>(Arrays.asList(Double.class, Double.class, Double.class,
//            Double.class, Double.class, Double.class, Double.class, Double.class));

    static List<String> fieldNames = new ArrayList<String>(Arrays.asList("id", "zcode", "payload"));
    static List<Class> valueTypes = new ArrayList<Class>(Arrays.asList(Double.class, Double.class, String.class));

    private static DataSchema schema = new DataSchema(fieldNames, valueTypes, "zcode");

    private DeserializationHelper() {
        schema = new DataSchema(fieldNames, valueTypes, "user_id");
    }

    public static BTreeLeafNode deserializeLeaf(byte [] b, int BTreeOrder, BytesCounter counter) throws IOException {
        BTreeLeafNode leaf = new BTreeLeafNode(BTreeOrder, counter);
        int len = Integer.SIZE / Byte.SIZE;
        int offset = 0;
        int keyCount = ByteBuffer.wrap(b, offset, len).getInt();

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
            for (int j = 0; j < tupleCount; ++j) {
                int lengthOfTuple = ByteBuffer.wrap(b, offset, len).getInt();
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


    public static BTree deserializeBTree(byte[] serializedTree, int BTreeOrder, BytesCounter counter) {
        BTreeInnerNode root = null;
        if (serializedTree.length > 0) {
            Queue<BTreeNode> q = new LinkedList<BTreeNode>();
            int len = Integer.SIZE / Byte.SIZE;
            root = new BTreeInnerNode(BTreeOrder, counter);
            int relativeOffset = 0;
            relativeOffset = deserialize(serializedTree, root, relativeOffset);
            q.add(root);

            int count = 0;
            BTreeInnerNode preNode = null;
            while (!q.isEmpty()) {
                BTreeInnerNode curr = (BTreeInnerNode) q.remove();

                for (int i = 0; i < curr.getKeyCount() + 1; ++i) {
                    BTreeInnerNode node = new BTreeInnerNode(BTreeOrder, counter);
                    relativeOffset = deserialize(serializedTree, node, relativeOffset);
                    if (node.getKeyCount() != 0) {
                        curr.setChild(i, node);
                    }
                    if (curr.offsets.size() == 0) {
                        q.add(node);
                    } else if (count == 0) {
                        preNode = curr;
                        ++count;
                    } else {
                        preNode.rightSibling = curr;
                        preNode = curr;
                        ++count;
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

    private static int deserialize(byte [] b, BTreeInnerNode node, int relativeOffset) {
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


    public static Values deserialize(byte[] b) throws IOException {
        return schema.deserialize(b);
    }
}
