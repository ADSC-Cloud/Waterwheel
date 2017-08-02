package indexingTopology.index;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import indexingTopology.compression.Compressor;
import indexingTopology.compression.CompressorFactory;
import indexingTopology.compression.Decompressor;
import indexingTopology.config.TopologyConfig;
import indexingTopology.index.BTreeLeafNode;


import java.io.IOException;
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

        Output localOutput;

        // If compression is enabled, we should first write the serialized bytes into a local buffer, then
        // compress it, and finally write it to the output.
        if (config.compression)
            localOutput = new Output(4096, 1024 * 1024 * 16);
        else
            localOutput = output;

        List<TKey> keys = bTreeLeafNode.getKeys();

        kryo.writeObject(localOutput, keys);

        for (int i = 0; i < bTreeLeafNode.getKeys().size(); i++) {
            localOutput.writeInt((bTreeLeafNode.getTuplesWithSpecificIndex(i)).size());
            for (int j = 0; j < (((ArrayList<byte []>) bTreeLeafNode.tuples.get(i)).size()); ++j) {
                localOutput.writeInt(((ArrayList<Integer>)bTreeLeafNode.offsets.get(i)).get(j));
                localOutput.write(((ArrayList<byte []>) bTreeLeafNode.tuples.get(i)).get(j));
            }
        }

        if (config.compression) {
            Compressor compressor = CompressorFactory.compressor(CompressorFactory.Algorithm.Snappy);
            try {
                byte[] compressed = compressor.compress(localOutput.toBytes());
                output.writeInt(compressed.length);
                output.writeBytes(compressed);
                localOutput.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    @Override
    public BTreeLeafNode read(Kryo kryo, Input input, Class<BTreeLeafNode> aClass) {

        BTreeLeafNode leaf = new BTreeLeafNode(config.BTREE_ORDER);

        Input localInput;

        // If compression is disabled, we simply get the leaf node from the input.
        // If compression is enabled, we first get the compressed data, then decompress the data, and finally get the
        // leaf node from the decompressed data.
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
        ArrayList keys = kryo.readObject(localInput, ArrayList.class);

        leaf.keys = keys;

        ArrayList<ArrayList<byte[]>> tuples = new ArrayList<ArrayList<byte[]>>();
        for (int i = 0; i < keys.size();i++) {
            int tupleCount = localInput.readInt();
            tuples.add(new ArrayList<>());
            for (int j = 0; j < tupleCount; ++j) {
                int lengthOfTuple = localInput.readInt();
                byte[] tuple = localInput.readBytes(lengthOfTuple);
                tuples.get(i).add(tuple);
            }
        }

        leaf.tuples = tuples;

        if (config.compression) {
            localInput.close();
        }

        return leaf;
    }



}
