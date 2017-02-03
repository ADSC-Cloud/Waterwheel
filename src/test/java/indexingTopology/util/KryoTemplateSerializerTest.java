package indexingTopology.util;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import indexingTopology.config.TopologyConfig;
import indexingTopology.exception.UnsupportedGenericException;
import indexingTopology.filesystem.FileSystemHandler;
import indexingTopology.filesystem.HdfsFileSystemHandler;
import indexingTopology.filesystem.LocalFileSystemHandler;
import indexingTopology.util.texi.Car;
import indexingTopology.util.texi.City;
import indexingTopology.util.texi.TrajectoryGenerator;
import indexingTopology.util.texi.TrajectoryUniformGenerator;
import org.apache.storm.tuple.Values;
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

    List<String> fieldNames = new ArrayList<String>(Arrays.asList("id", "zcode", "payload"));
    List<Class> valueTypes = new ArrayList<Class>(Arrays.asList(Double.class, Double.class, String.class));

    @Test
    public void testDeserialize() throws IOException, UnsupportedGenericException {

        final int payloadSize = 10;

        final double x1 = 0;
        final double x2 = 1000;
        final double y1 = 0;
        final double y2 = 500;
        final int partitions = 100;

        TrajectoryGenerator generator = new TrajectoryUniformGenerator(10000, x1, x2, y1, y2);
        City city = new City(x1, x2, y1, y2, partitions);

        int numTuples = TopologyConfig.NUMBER_TUPLES_OF_A_CHUNK;
        Long timestamp = 0L;

        int chunkId = 0;

        List<String> fileNames = new ArrayList<>();

        BTree indexedData = new BTree(TopologyConfig.BTREE_ORDER);

        int numberOfTuples = 60;

        Random random = new Random();

        List<Integer> keys = new ArrayList<>();

        for (int i = 0; i < numTuples; ++i) {
            List<Object> values = new ArrayList<>();
            Car car = generator.generate();
            values.add((double) car.id);
            values.add((double) city.getZCodeForALocation(car.x, car.y));
            values.add(new String(new char[payloadSize]));
            values.add(timestamp);
            byte[] bytes = null;
            bytes = serializeIndexValue(values);
            indexedData.insert((double) car.id, bytes);
        }


        indexedData.serializeLeaves();

        Output output = new Output(500000);

        Kryo kryo = new Kryo();
        kryo.register(BTree.class, new KryoTemplateSerializer());

        kryo.writeObject(output, indexedData);

        byte[] bytes = output.toBytes();

        Input input = new Input(bytes);

        indexedData = kryo.readObject(input, BTree.class);

    }

    @Test
    public void testTemplateAndLeaveDeserialization() throws IOException, UnsupportedGenericException {
        final int payloadSize = 10;

        final double x1 = 0;
        final double x2 = 1000;
        final double y1 = 0;
        final double y2 = 500;
        final int partitions = 100;

        TrajectoryGenerator generator = new TrajectoryUniformGenerator(10000, x1, x2, y1, y2);
        City city = new City(x1, x2, y1, y2, partitions);

        int numTuples = TopologyConfig.NUMBER_TUPLES_OF_A_CHUNK;
        Long timestamp = 0L;

        int chunkId = 0;

        List<String> fileNames = new ArrayList<>();

        BTree indexedData = new BTree(TopologyConfig.BTREE_ORDER);

        int numberOfTuples = 60;

        Random random = new Random();

        List<Integer> keys = new ArrayList<>();

        for (int i = 0; i < numTuples; ++i) {
            List<Object> values = new ArrayList<>();
            Car car = generator.generate();
            values.add((double) car.id);
            values.add((double) city.getZCodeForALocation(car.x, car.y));
            values.add(new String(new char[payloadSize]));
            values.add(timestamp);
            byte[] bytes = null;
            bytes = serializeIndexValue(values);
            indexedData.insert((double) car.id, bytes);
        }


        byte[] leavesInBytes = indexedData.serializeLeaves();

        Output output = new Output(500000, 20000000);

        Kryo kryo = new Kryo();
        kryo.register(BTree.class, new KryoTemplateSerializer());
        kryo.register(BTreeLeafNode.class, new KryoLeafNodeSerializer());

        kryo.writeObject(output, indexedData);

        byte[] bytes = output.toBytes();

        int lengthOfTemplate = bytes.length;

        output = new Output(4);

        output.writeInt(lengthOfTemplate);

        byte[] lengthInBytes = output.toBytes();

        MemChunk chunk = MemChunk.createNew(4 + lengthOfTemplate + leavesInBytes.length);

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

        indexedData = kryo.readObject(input, BTree.class);

        indexedData.printBtree();

        BTreeNode mostLeftNode = indexedData.findLeafNodeShouldContainKeyInDeserializedTemplate(0.0);
        BTreeNode mostRightNode = indexedData.findLeafNodeShouldContainKeyInDeserializedTemplate(60.0);

        List<Integer> offsets = indexedData.getOffsetsOfLeaveNodesShouldContainKeys(mostLeftNode
                , mostRightNode);

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

    @Test
    public void testTemplateUpdateDeserialization() throws UnsupportedGenericException, IOException {
        final int payloadSize = 10;

        final double x1 = 0;
        final double x2 = 1000;
        final double y1 = 0;
        final double y2 = 500;
        final int partitions = 100;

        TrajectoryGenerator generator = new TrajectoryUniformGenerator(10000, x1, x2, y1, y2);
        City city = new City(x1, x2, y1, y2, partitions);

        int numTuples = TopologyConfig.NUMBER_TUPLES_OF_A_CHUNK;
        Long timestamp = 0L;

        int chunkId = 0;

        List<String> fileNames = new ArrayList<>();

        BTree indexedData = new BTree(TopologyConfig.BTREE_ORDER);

        while (chunkId <= 1) {
            for (int i = 0; i < numTuples; ++i) {
                List<Object> values = new ArrayList<>();
                Car car = generator.generate();
                values.add((double) car.id);
                values.add((double) city.getZCodeForALocation(car.x, car.y));
                values.add(new String(new char[payloadSize]));
                values.add(timestamp);
                byte[] bytes = null;
                bytes = serializeIndexValue(values);
                indexedData.insert((double) car.id, bytes);
            }

            byte[] leavesInBytes = indexedData.serializeLeaves();

            Output output = new Output(5000000, 20000000);

            Kryo kryo = new Kryo();
            kryo.register(BTree.class, new KryoTemplateSerializer());
            kryo.register(BTreeLeafNode.class, new KryoLeafNodeSerializer());

            kryo.writeObject(output, indexedData);

            byte[] bytes = output.toBytes();

            int lengthOfTemplate = bytes.length;

            output = new Output(4);

            output.writeInt(lengthOfTemplate);

            byte[] lengthInBytes = output.toBytes();

            MemChunk chunk = MemChunk.createNew(4 + lengthOfTemplate + leavesInBytes.length);

            chunk.write(lengthInBytes);

            chunk.write(bytes);

            chunk.write(leavesInBytes);

            FileSystemHandler fileSystemHandler = null;


            try {
                String fileName = null;
                if (TopologyConfig.HDFSFlag) {
                    fileSystemHandler = new HdfsFileSystemHandler(TopologyConfig.dataDir);
                } else {
                    fileSystemHandler = new LocalFileSystemHandler(TopologyConfig.dataDir);
                }

                fileName = "taskId" + 0 + "chunk" + chunkId;
                fileNames.add(fileName);
                fileSystemHandler.writeToFileSystem(chunk, "/", fileName);
            } catch (IOException e) {
                e.printStackTrace();
            }

            indexedData = indexedData.clone();

            indexedData.clearPayload();

            indexedData.printBtree();

            for (String fileName : fileNames) {

                byte[] temlateLengthInBytes = new byte[4];

                fileSystemHandler.openFile("/", fileName);

                fileSystemHandler.readBytesFromFile(temlateLengthInBytes);

                Input input = new Input(temlateLengthInBytes);

                int length = input.readInt();

                byte[] templateInBytes = new byte[length];

                fileSystemHandler.readBytesFromFile(templateInBytes);

                input = new Input(templateInBytes);

                BTree bTree = kryo.readObject(input, BTree.class);

                BTreeNode mostLeftNode = bTree.findLeafNodeShouldContainKeyInDeserializedTemplate(994.0);
                BTreeNode mostRightNode = bTree.findLeafNodeShouldContainKeyInDeserializedTemplate(1000.0);

                List<Integer> offsets = indexedData.getOffsetsOfLeaveNodesShouldContainKeys(mostLeftNode
                        , mostRightNode);

                System.out.println(offsets);

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

                    ArrayList<byte[]> tuples = leaf.getTuples(994.0, 994.0);
//                ArrayList<byte[]> tuples = leaf.searchAndGetTuples(994.0);

                    for (int j = 0; j < tuples.size(); ++j) {
                        System.out.println(deserialize(tuples.get(j)));
                    }

                }
                fileSystemHandler.closeFile();
            }

            ++chunkId;
        }

    }

    @Test
    public void testWriteString() {
        Double a = 4.0;
        String s = new String(new char[5]);
        Output output = new Output(1000, 20000);
        output.writeDouble(a);
        output.writeString(s);
        byte[] bytes = output.toBytes();
        Input input = new Input(bytes);

        System.out.println(input.readDouble());
        System.out.println(input.readString());
    }

    public byte[] serializeIndexValue(List<Object> values) throws IOException {
        ByteArrayOutputStream bos = new ByteArrayOutputStream();

        for (int i = 0;i < valueTypes.size(); ++i) {
            if (valueTypes.get(i).equals(Double.class)) {
                byte [] b = ByteBuffer.allocate(Double.SIZE / Byte.SIZE).putDouble((Double) values.get(i)).array();
                bos.write(b);
            } else if (valueTypes.get(i).equals(String.class)) {
                byte [] b = ((String) values.get(i)).getBytes();
                byte [] sizeHeader = ByteBuffer.allocate(Integer.SIZE/ Byte.SIZE).putInt(b.length).array();
                bos.write(sizeHeader);
                bos.write(b);
            } else {
                throw new IOException("Only classes supported till now are string and double");
            }
        }

        byte [] b = ByteBuffer.allocate(Long.SIZE / Byte.SIZE).putLong((Long) values.get(valueTypes.size())).array();
        bos.write(b);
        return bos.toByteArray();
    }

    public Values deserialize(byte [] b) throws IOException {
        Values values = new Values();
        int offset = 0;
        for (int i = 0; i < valueTypes.size(); i++) {
            if (valueTypes.get(i).equals(Double.class)) {
                int len = Double.SIZE/Byte.SIZE;
                double val = ByteBuffer.wrap(b, offset, len).getDouble();
                values.add(val);
                offset += len;
            } else if (valueTypes.get(i).equals(String.class)) {
                int len = Integer.SIZE/Byte.SIZE;
                int sizeHeader = ByteBuffer.wrap(b, offset, len).getInt();
                offset += len;
                len = sizeHeader;
                String val = new String(b, offset, len);
                values.add(val);
                offset += len;
            } else {
                throw new IOException("Only classes supported till now are string and double");
            }
        }

        int len = Long.SIZE / Byte.SIZE;
        Long val = ByteBuffer.wrap(b, offset, len).getLong();
        values.add(val);
        return values;
    }

}