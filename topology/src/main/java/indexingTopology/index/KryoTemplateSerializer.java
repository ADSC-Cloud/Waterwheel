package indexingTopology.index;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import indexingTopology.compression.Compressor;
import indexingTopology.compression.CompressorFactory;
import indexingTopology.compression.Decompressor;
import indexingTopology.config.TopologyConfig;
import indexingTopology.index.BTree;
import indexingTopology.index.BTreeInnerNode;
import indexingTopology.index.BTreeNode;
import indexingTopology.index.TreeNodeType;

import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.Queue;

/**
 * Created by acelzj on 1/4/17.
 */
public class KryoTemplateSerializer<TKey extends Comparable<TKey>> extends Serializer<BTree> {

    TopologyConfig config;

    public KryoTemplateSerializer(TopologyConfig config) {
        this.config = config;
    }

    @Override
    public void write(Kryo kryo, Output output, BTree bTree) {
        Output localOutput;
        if (config.compression) {
            localOutput = new Output(4096, 1024 * 1024 * 4);
        } else {
            localOutput = output;
        }
        Queue<BTreeNode> q = new LinkedList<BTreeNode>();
        q.add(bTree.getRoot());

        while (!q.isEmpty()) {
            BTreeInnerNode curr = (BTreeInnerNode) q.remove();
            localOutput.write(serializeInnerNode(curr));
            if (curr.getChildren().size() > 0 && curr.getChild(0).getNodeType() == TreeNodeType.InnerNode) {
                q.addAll(curr.getChildren());
            }
        }

        if (config.compression) {
            byte[] data = localOutput.toBytes();
            Compressor compressor = CompressorFactory.compressor(CompressorFactory.Algorithm.Snappy);
            try {
                byte[] compressed = compressor.compress(data);
                output.writeInt(compressed.length);
                output.writeBytes(compressed);
                localOutput.close();
            } catch (IOException e) {
                e.printStackTrace();
                return;
            }
        }

    }

    @Override
    public BTree read(Kryo kryo, Input input, Class<BTree> aClass) {
        BTreeInnerNode root = null;
        Queue<BTreeNode> q = new LinkedList<BTreeNode>();

        Input localInput;

        if (config.compression) {
            int compressedLength = input.readInt();
            byte[] compressed = input.readBytes(compressedLength);
            Decompressor decompressor = CompressorFactory.decompressor(CompressorFactory.Algorithm.Snappy);
            try {
                byte[] decompressed = decompressor.decompress(compressed);
                localInput = new Input(decompressed);
            } catch (IOException e) {
                e.printStackTrace();
                return null;
            }

        } else {
            localInput = input;
        }


        root = new BTreeInnerNode(config.BTREE_ORDER);
        deserialize(localInput, root);
        if (root.getOffsets().size() == 0) {
            q.add(root);
        }

        int count = 0;
        BTreeInnerNode preNode = null;
        while (!q.isEmpty()) {
            BTreeInnerNode curr = (BTreeInnerNode) q.remove();
            for (int i = 0; i < curr.getKeyCount() + 1; ++i) {
                BTreeInnerNode node = new BTreeInnerNode(config.BTREE_ORDER);
                deserialize(localInput, node);


                if (node.getKeyCount() != 0) {
                    curr.setChild(i, node);
                }


                if (node.getOffsets().size() == 0) {
                    q.add(node);
                } else if (count == 0) {
                    preNode = node;
                    ++count;
                } else {
                    preNode.setRightSibling(node);
                    preNode = node;
                    ++count;
                }
            }
        }

        if (config.compression) {
            localInput.close();
        }

        BTree bTree = new BTree(config.BTREE_ORDER, config);
        bTree.setRoot(root);
        return bTree;
    }

    private byte[] serializeInnerNode(BTreeInnerNode node) {

        Output output = new Output(500000, 200000000);


        Kryo kryo = new Kryo();
        ArrayList<TKey> keys = node.getKeys();
        kryo.writeObject(output, keys);


        if (node.offsets.size() != 0) {
            output.writeChar('y');
            kryo.writeObject(output, node.getOffsets());
        } else {
            output.writeChar('n');
        }


        byte[] bytes = output.toBytes();
        output.close();
        return bytes;
    }

    private void deserialize(Input input, BTreeInnerNode node) {
        Kryo kryo = new Kryo();
        ArrayList keys = kryo.readObject(input, ArrayList.class);

        node.keys = keys;

        ArrayList<Integer> offsets = new ArrayList<>();

        char haveOffsets = input.readChar();
        if (haveOffsets == 'y') {
            offsets = kryo.readObject(input, ArrayList.class);
        }

        node.offsets = offsets;
    }
}
