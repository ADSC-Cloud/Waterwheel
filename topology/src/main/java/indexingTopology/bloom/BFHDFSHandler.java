package indexingTopology.bloom;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.IOUtils;

import java.io.*;
import java.net.URI;
import java.net.URISyntaxException;


/**
 * Created by john on 30/4/17.
 */
public class BFHDFSHandler {
    private static final String LOCAL_HOST = "hdfs://localhost:9000/";

    // read data from HDFS
    public static void readHDFS(String localFileName) throws IOException, URISyntaxException {

        final Path path = new Path(localFileName);
        final Configuration conf = new Configuration();
        conf.set("dfs.replication", "1");
        final FileSystem fs = FileSystem.get(URI.create(LOCAL_HOST), conf);

        OutputStream out = new BufferedOutputStream(new FileOutputStream(localFileName));
        FSDataInputStream in = fs.open(path);
        IOUtils.copyBytes(in, out, 4096, false);

        out.close();
        fs.close();

        System.out.println("reading bloom filter from HDFS finished!");
    }

    // write data to HDFS
    public static void writeHDFS(String localFileName) throws IOException, URISyntaxException {

        final Path path = new Path(localFileName);
        final Configuration conf = new Configuration();
        conf.set("dfs.replication", "1");
        final FileSystem fs = FileSystem.get(URI.create(LOCAL_HOST), conf);

        InputStream in = new BufferedInputStream(new FileInputStream(localFileName));
        FSDataOutputStream out = fs.create(path);
        IOUtils.copyBytes(in, out, 4096, true);

        in.close();
        out.close();
        fs.close();

        System.out.println("writing bloom filter to HDFS finished!");
    }

    // delete existing file in HDFS
    public static void deleteFile(String localFileName) throws IOException, URISyntaxException {

        final Path path = new Path(localFileName);
        final Configuration conf = new Configuration();
        conf.set("dfs.replication", "1");

        final FileSystem fs = FileSystem.get(URI.create(LOCAL_HOST), conf);
        fs.deleteOnExit(path);

        fs.close();

        System.out.println("deleting bloom filter file finished!");
    }
}