package indexingTopology.util;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import indexingTopology.config.TopologyConfig;


import java.util.ArrayList;
import java.util.List;

/**
 * Created by acelzj on 1/4/17.
 */
public class KryoLeafNodeSerializer<TKey extends Comparable<TKey>> extends Serializer<BTreeLeafNode> {

    private TopologyConfig config;

    public KryoLeafNodeSerializer(TopologyConfig config) {
        this.config = config;
    }


    @Override
    public void write(Kryo kryo, Output output, BTreeLeafNode bTreeLeafNode) {

        List<TKey> keys = bTreeLeafNode.getKeys();
//        output.writeInt(keys.size());

//        for (TKey key : keys) {
//            if (key instanceof Integer) {
//                output.writeInt((Integer) key);
//            } else if (key instanceof Double) {
//                output.writeDouble((Double) key);
//            }
//        }
        kryo.writeObject(output, keys);

        for (int i = 0; i < bTreeLeafNode.getKeys().size(); i++) {
            output.writeInt((bTreeLeafNode.getTuplesWithSpecificIndex(i)).size());
            for (int j = 0; j < (((ArrayList<byte []>) bTreeLeafNode.tuples.get(i)).size()); ++j) {
                output.writeInt(((ArrayList<Integer>)bTreeLeafNode.offsets.get(i)).get(j));
                output.write(((ArrayList<byte []>) bTreeLeafNode.tuples.get(i)).get(j));
            }
        }
    }

    @Override
    public BTreeLeafNode read(Kryo kryo, Input input, Class<BTreeLeafNode> aClass) {

        BTreeLeafNode leaf = new BTreeLeafNode(config.BTREE_ORDER);

        ArrayList keys = kryo.readObject(input, ArrayList.class);

        leaf.keys = keys;
//        int keyCount = input.readInt();

//        ArrayList keys = null;

//        if (TopologyConfig.KEY_TPYE.equals("double")) {
//            keys = new ArrayList<Double>();
//
//            for (int i = 0; i < keyCount; ++i) {
//                Double key = input.readDouble();
//                keys.add(key);
//            }
//
//            leaf.keys = keys;
//        } else if (TopologyConfig.KEY_TPYE.equals("int")) {
//            keys = new ArrayList<Integer>();
//
//            for (int i = 0; i < keyCount; ++i) {
//                Integer key = input.readInt();
//                keys.add(key);
//            }
//
//            leaf.keys = keys;
//        }

        ArrayList<ArrayList<byte[]>> tuples = new ArrayList<ArrayList<byte[]>>();
        for (int i = 0; i < keys.size();i++) {
            int tupleCount = input.readInt();
            tuples.add(new ArrayList<>());
            for (int j = 0; j < tupleCount; ++j) {
                int lengthOfTuple = input.readInt();
                byte[] tuple = input.readBytes(lengthOfTuple);
                tuples.get(i).add(tuple);
            }
        }

        leaf.tuples = tuples;

        return leaf;
    }



}
