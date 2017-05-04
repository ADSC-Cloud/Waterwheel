package indexingTopology.bloom;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.IOUtils;

import java.io.*;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.ByteBuffer;

/**
 * Created by john on 30/4/17.
 */
public class BFHDFSHandler {
    private static final String LOCAL_HOST = "hdfs://localhost:9000/";
    private static final String FILE_PATH = "bf.dat";

    // read data from HDFS
    public static void readHDFS(String fileName) throws IOException, URISyntaxException {
        final Path path = new Path(FILE_PATH);
        final Configuration conf = new Configuration();
        conf.set("dfs.replication", "1");
        conf.setBoolean("dfs.support.append", true);
        final FileSystem fs = FileSystem.get(URI.create(LOCAL_HOST), conf);

        OutputStream out = new BufferedOutputStream(new FileOutputStream(fileName));

        final FileStatus fileStatus = fs.getFileStatus(path);
        long len = fileStatus.getLen();

        System.out.println("length of file: " + len);

        ByteBuffer buffer = ByteBuffer.allocate((int) len);
        FSDataInputStream in = fs.open(path);

        IOUtils.copyBytes(in, out, 4096, false);

        out.close();
        fs.close();
        System.out.println("read file finished!");
    }

    // write data to HDFS
    public static void writeHDFS(String fileName) throws IOException, URISyntaxException {
        final Path path = new Path(FILE_PATH);
        final Configuration conf = new Configuration();
        conf.set("dfs.replication", "1");
        conf.setBoolean("dfs.support.append", true);
        final FileSystem fs = FileSystem.get(URI.create(LOCAL_HOST), conf);

        InputStream in = new BufferedInputStream(new FileInputStream(fileName));
        FSDataOutputStream out = null;
        if (!fs.exists(path)) {
            fs.create(path).close();
        }
        out = fs.append(path);

        IOUtils.copyBytes(in, out, 4096, true);
        in.close();
        out.close();
        fs.close();

        System.out.println("write file finished!");
    }

    public static void deleteFile() throws IOException, URISyntaxException {
        final Path path = new Path(FILE_PATH);
        final Configuration conf = new Configuration();
        conf.set("dfs.replication", "1");
        conf.setBoolean("dfs.support.append", true);
        final FileSystem fs = FileSystem.get(URI.create(LOCAL_HOST), conf);
        fs.deleteOnExit(path);
        fs.close();
        System.out.println("delete file finished!");
    }
}