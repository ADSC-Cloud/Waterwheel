package indexingTopology.util;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import indexingTopology.config.TopologyConfig;
import indexingTopology.exception.UnsupportedGenericException;
import indexingTopology.filesystem.FileSystemHandler;
import indexingTopology.filesystem.HdfsFileSystemHandler;
import indexingTopology.filesystem.LocalFileSystemHandler;
import indexingTopology.util.*;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;

/**
 * Created by acelzj on 1/4/17.
 */
public class KryoTemplateSerializerTest {

    private List<String> fieldNames = new ArrayList<String>(Arrays.asList("user_id", "id_1", "id_2", "ts_epoch",
            "date", "time", "latitude", "longitude"));
    private ArrayList valueTypes = new ArrayList<Class>(Arrays.asList(Double.class, Double.class, Double.class,
            Double.class, Double.class, Double.class, Double.class, Double.class));

    @Test
    public void testDeserialize() throws IOException, UnsupportedGenericException {

        int order = TopologyConfig.BTREE_OREDER;
        BTree bTree = new BTree(order, TimingModule.createNew(), SplitCounterModule.createNew());

        int numberOfTuples = 1024;

        Random random = new Random();

        List<Integer> keys = new ArrayList<>();

        for (int i = 0; i < numberOfTuples; ++i) {

            List<Double> values = new ArrayList<>();
            values.add((double) i);
            for (int j = 0; j < fieldNames.size() + 1; ++j) {
                values.add((double) j);
            }
            byte[] bytes = serializeIndexValue(values);
            bTree.insert((double) i, bytes);

        }


        bTree.serializeLeaves();

        Output output = new Output(500000);

        Kryo kryo = new Kryo();
        kryo.register(BTree.class, new KryoTemplateSerializer());

        kryo.writeObject(output, bTree);

        byte[] bytes = output.toBytes();

        Input input = new Input(bytes);

        bTree = kryo.readObject(input, BTree.class);

    }

    @Test
    public void testTemplateAndLeaveDeserialization() throws IOException, UnsupportedGenericException {
        int order = TopologyConfig.BTREE_OREDER;
        BTree bTree = new BTree(order, TimingModule.createNew(), SplitCounterModule.createNew());

        int numberOfTuples = 60;

        Random random = new Random();

        List<Integer> keys = new ArrayList<>();

        for (int i = 0; i < numberOfTuples; ++i) {

            List<Double> values = new ArrayList<>();
            values.add((double) i);
            for (int j = 0; j < fieldNames.size() + 1; ++j) {
                values.add((double) j);
            }
            byte[] bytes = serializeIndexValue(values);
            bTree.insert((double) i, bytes);

        }


        byte[] leavesInBytes = bTree.serializeLeaves();

        Output output = new Output(500000);

        Kryo kryo = new Kryo();
        kryo.register(BTree.class, new KryoTemplateSerializer());
        kryo.register(BTreeLeafNode.class, new KryoLeafNodeSerializer());

        kryo.writeObject(output, bTree);

        byte[] bytes = output.toBytes();

        MemChunk chunk = MemChunk.createNew(6500000);

        int lengthOfTemplate = bytes.length;

        output = new Output(4);

        output.writeInt(lengthOfTemplate);

        byte[] lengthInBytes = output.toBytes();

        chunk.write(lengthInBytes );

        chunk.write(bytes);

        chunk.write(leavesInBytes);

        FileSystemHandler fileSystemHandler = null;

        String fileName = null;

        try {
            if (TopologyConfig.HDFSFlag) {
                fileSystemHandler = new HdfsFileSystemHandler(TopologyConfig.dataDir);
            } else {
                fileSystemHandler = new LocalFileSystemHandler(TopologyConfig.dataDir);
            }

            fileName = "taskId" + 0 + "chunk" + 0;
            fileSystemHandler.writeToFileSystem(chunk, "/", fileName);
        } catch (IOException e) {
            e.printStackTrace();
        }

        byte[] temlateLengthInBytes = new byte[4];

        fileSystemHandler.openFile("/", "taskId0chunk0");

        fileSystemHandler.readBytesFromFile(temlateLengthInBytes);

        Input input = new Input(temlateLengthInBytes);

        int length = input.readInt();

        byte[] templateInBytes = new byte[length];

        fileSystemHandler.readBytesFromFile(templateInBytes);

        input = new Input(templateInBytes);

        bTree = kryo.readObject(input, BTree.class);

        BTreeNode mostLeftNode = bTree.findLeafNodeShouldContainKeyInDeserializedTemplate(0.0);
        BTreeNode mostRightNode = bTree.findLeafNodeShouldContainKeyInDeserializedTemplate(60.0);

        Long searchStartTime = System.currentTimeMillis();
        List<Integer> offsets = bTree.getOffsetsOfLeaveNodesShouldContainKeys(mostLeftNode
                , mostRightNode);

        /*
        for (int i = 0; i < numberOfTuples; ++i) {

            int offset = bTree.getOffsetOfLeaveNodeShouldContainKey((double) i);

            offset += (length + 4);

            System.out.println(offset);

            fileSystemHandler.seek(offset);

            byte[] lengthInByte = new byte[4];

            fileSystemHandler.readBytesFromFile(offset, lengthInByte);

            int lengthOfLeaveInBytes = ByteBuffer.wrap(lengthInByte, 0, 4).getInt();

            byte[] leafInByte = new byte[lengthOfLeaveInBytes + 1];

            fileSystemHandler.seek(offset + 4);

            fileSystemHandler.readBytesFromFile(leafInByte);

            input = new Input(leafInByte);

            BTreeLeafNode leaf = kryo.readObject(input, BTreeLeafNode.class);

            leaf.print();
        }
        */

        for (Integer offset : offsets) {

            fileSystemHandler.seek(offset + length + 4);

            byte[] lengthInByte = new byte[4];

            fileSystemHandler.readBytesFromFile(offset + length + 4, lengthInByte);

            int lengthOfLeaveInBytes = ByteBuffer.wrap(lengthInByte, 0, 4).getInt();

            byte[] leafInByte = new byte[lengthOfLeaveInBytes + 1];

            fileSystemHandler.seek(offset + length + 4 + 4);

            fileSystemHandler.readBytesFromFile(leafInByte);

            input = new Input(leafInByte);

            BTreeLeafNode leaf = kryo.readObject(input, BTreeLeafNode.class);

            leaf.print();
        }
    }

    public byte[] serializeIndexValue(List<Double> values) throws IOException {
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        ArrayList valueTypes = new ArrayList<Class>(Arrays.asList(Double.class, Double.class, Double.class,
                Double.class, Double.class, Double.class, Double.class, Double.class));
        for (int i = 0;i < valueTypes.size(); ++i) {
            if (valueTypes.get(i).equals(Double.class)) {
                byte [] b = ByteBuffer.allocate(Double.SIZE / Byte.SIZE).putDouble((Double) values.get(i)).array();
                bos.write(b);
            }
        }

        byte [] b = ByteBuffer.allocate(Long.SIZE / Byte.SIZE).putDouble(System.currentTimeMillis()).array();
        bos.write(b);
        return bos.toByteArray();
    }


}