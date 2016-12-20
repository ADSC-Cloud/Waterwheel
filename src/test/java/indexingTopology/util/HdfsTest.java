package indexingTopology.util;

import indexingTopology.Config.TopologyConfig;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.net.URI;
import java.nio.ByteBuffer;

/**
 * Created by acelzj on 12/5/16.
 */
public class HdfsTest {

    @Test
    public void testHdfsWrite() throws IOException {

        ByteArrayOutputStream bos = new ByteArrayOutputStream();

        for (int i = 1; i < 64000; ++i) {
            byte [] b = ByteBuffer.allocate(Integer.SIZE / Byte.SIZE).putInt(i).array();
            writeToByteArrayOutputStream(bos, b);
        }

        byte[] bytes = bos.toByteArray();
        int size = bytes.length;

        System.out.println("size" + size);

        FSDataInputStream fsDataInputStream = null;
        FileSystem fileSystem;
        Configuration configuration = new Configuration();
        configuration.setBoolean("dfs.support.append", true);
        URI uri = URI.create(TopologyConfig.HDFS_HOST + "/src");
        fileSystem = FileSystem.get(uri, configuration);
        Path path = new Path("/src/testHdfs");
        FSDataOutputStream fsDataOutputStream = fileSystem.create(path);
        fsDataOutputStream.write(bytes);
        fsDataOutputStream.close();

        try {
            fsDataInputStream = fileSystem.open(path);
        } catch (IOException e) {
            e.printStackTrace();
        }

        bytes = new byte[size];
//        byte[] testBytes = new byte[size];
        System.out.println("Length " + bytes.length);
        fsDataInputStream.readFully(0, bytes);
        int offset = 0;
        Integer number1 = ByteBuffer.wrap(bytes, 65536, 4).getInt();
        System.out.println("number1" + number1);
//        while (offset < bytes.length) {
//            Integer number = ByteBuffer.wrap(bytes, offset, 4).getInt();
//            System.out.println(number);
//            if (number == 0) {
//                break;
//            }
//            offset += 4;
//        }
    }

    private static void writeToByteArrayOutputStream(ByteArrayOutputStream bos, byte[] b) {
        try {
            bos.write(b);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
