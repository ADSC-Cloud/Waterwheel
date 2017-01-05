package indexingTopology.util;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import indexingTopology.config.TopologyConfig;
import indexingTopology.util.*;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;

/**
 * Created by acelzj on 1/4/17.
 */
public class KryoTemplateSerializer<TKey extends Comparable<TKey>> extends Serializer<BTree> {


    @Override
    public void write(Kryo kryo, Output output, BTree bTree) {
        Queue<BTreeNode> q = new LinkedList<BTreeNode>();
        q.add(bTree.getRoot());

        while (!q.isEmpty()) {
            BTreeInnerNode curr = (BTreeInnerNode) q.remove();
            output.write(serializeInnerNode(curr));
            if (curr.getChildren().size() > 0 && curr.getChild(0).getNodeType() == TreeNodeType.InnerNode) {
                q.addAll(curr.getChildren());
            }
        }

    }

    @Override
    public BTree read(Kryo kryo, Input input, Class<BTree> aClass) {
        BTreeInnerNode root = null;
        Queue<BTreeNode> q = new LinkedList<BTreeNode>();
        BytesCounter counter = new BytesCounter();
        root = new BTreeInnerNode(TopologyConfig.BTREE_OREDER, counter);
        deserialize(input, root);
        q.add(root);

        int count = 0;
        BTreeInnerNode preNode = null;
        while (!q.isEmpty()) {
            BTreeInnerNode curr = (BTreeInnerNode) q.remove();

            for (int i = 0; i < curr.getKeyCount() + 1; ++i) {
                BTreeInnerNode node = new BTreeInnerNode(TopologyConfig.BTREE_OREDER, counter);
                deserialize(input, node);
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

        TimingModule tm = TimingModule.createNew();
        SplitCounterModule sm = SplitCounterModule.createNew();
        BTree bTree = new BTree(TopologyConfig.BTREE_OREDER, tm, sm);
        bTree.setRoot(root);
        return bTree;
    }

    private byte[] serializeInnerNode(BTreeInnerNode node) {

        Output output = new Output(50000);

        ArrayList<TKey> keys = node.getKeys();

        Kryo kryo = new Kryo();
        kryo.writeObject(output, keys);

        if (node.offsets.size() != 0) {
            output.writeChar('y');
            kryo.writeObject(output, node.getOffsets());
        } else {
            output.writeChar('n');
        }

        return output.toBytes();
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
