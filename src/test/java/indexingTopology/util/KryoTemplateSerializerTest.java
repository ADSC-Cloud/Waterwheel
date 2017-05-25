package indexingTopology.util;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import indexingTopology.config.TopologyConfig;
import indexingTopology.exception.UnsupportedGenericException;
import indexingTopology.filesystem.FileSystemHandler;
import indexingTopology.filesystem.HdfsFileSystemHandler;
import indexingTopology.filesystem.LocalFileSystemHandler;
import indexingTopology.util.taxi.Car;
import indexingTopology.util.taxi.City;
import indexingTopology.util.taxi.TrajectoryGenerator;
import indexingTopology.util.taxi.TrajectoryUniformGenerator;
import junit.framework.TestCase;
import org.apache.storm.tuple.Values;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;

import static org.junit.Assert.assertEquals;

/**
 * Created by acelzj on 1/4/17.
 */
public class KryoTemplateSerializerTest extends TestCase {

    List<String> fieldNames = new ArrayList<String>(Arrays.asList("id", "zcode", "payload"));
    List<Class> valueTypes = new ArrayList<Class>(Arrays.asList(Double.class, Double.class, String.class));

    private TopologyConfig config = new TopologyConfig();

    public void setUp() {
        try {
            Runtime.getRuntime().exec("mkdir -p ./target/tmp");
        } catch (IOException e) {
            e.printStackTrace();
        }
        config.dataDir = "./target/tmp";
        config.HDFSFlag = false;
        config.CHUNK_SIZE = 4 * 1000 * 1000;
        System.out.println("dataDir is set to " + config.dataDir);
    }

    public void tearDown() {
        try {
            Runtime.getRuntime().exec("rm ./target/tmp/*");
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

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

        int numTuples = (int)(config.CHUNK_SIZE / (8 * 3 + payloadSize) * 1.3);
        Long timestamp = 0L;

        int chunkId = 0;

        List<String> fileNames = new ArrayList<>();

        BTree indexedData = new BTree(config.BTREE_ORDER, config);

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
        kryo.register(BTree.class, new KryoTemplateSerializer(config));

        kryo.writeObject(output, indexedData);

        byte[] bytes = output.toBytes();

        Input input = new Input(bytes);

        indexedData = kryo.readObject(input, BTree.class);

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