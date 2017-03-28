package indexingTopology.metadata;

import indexingTopology.config.TopologyConfig;

import com.github.davidmoten.rtree.RTree;
import com.github.davidmoten.rtree.geometry.Geometries;
import com.github.davidmoten.rtree.geometry.Rectangle;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.ByteBuffer;


/**
 * Created by john on 26/3/17.
 */
public class LoggingTable {

    /**
     * countOfEntries: total number of meta log items in HDFS
     * sizeInBytes: total size in bytes of meta log, used for output purpose
     * min/maxChildren: m & M parameters for RTree node
     */
    private static long countOfEntries = 0;
    private static long sizeInBytes = 0;
    private static final int minChildren = 2;
    private static final int maxChildren = 4;
    private static RTree<FileMetaData, Rectangle> rTree = RTree.minChildren(minChildren).maxChildren(maxChildren).create();


    /**
     * Get the RTree maintained by LoggingTable
     * @return a RTree object constructed by appending operations
     */
    public static RTree<FileMetaData, Rectangle> getRTree() {
        return rTree;
    }

    /**
     * append a meta data item to the meta log file. If the file does not exist,
     * create a new file defined in {@link TopologyConfig#HDFS_META_LOG_NAME}
     */
    public static synchronized void append(FileMetaData fileMetaData) throws IOException {

        // add it to RTree first
        rTree = rTree.add(fileMetaData, Geometries.rectangle(fileMetaData.keyRangeLowerBound, fileMetaData.startTime,
                fileMetaData.keyRangeUpperBound, fileMetaData.endTime));

        Configuration conf = new Configuration();
        conf.set("dfs.replication", "1");
        conf.setBoolean("dfs.support.append", true);
        FileSystem fileSystem = null;
        try {
            fileSystem = FileSystem.get(new URI(TopologyConfig.HDFS_HOST_LOCAL), conf);
        } catch (URISyntaxException e) {
            e.printStackTrace();
        }
        Path path = new Path(TopologyConfig.HDFS_META_LOG_PATH);

        // create a new file if it doesn't exist
        if (!fileSystem.exists(path)) {
            fileSystem.create(path).close();
        }

        // write FileMetaData into ByteBuffer
        int allocatedSize = 64;
        ByteBuffer buffer = ByteBuffer.allocate(allocatedSize);
        buffer.putInt(fileMetaData.filename.getBytes().length);
        buffer.put(fileMetaData.filename.getBytes());
        buffer.putDouble(fileMetaData.keyRangeLowerBound);
        buffer.putLong(fileMetaData.startTime);
        buffer.putDouble(fileMetaData.keyRangeUpperBound);
        buffer.putLong(fileMetaData.endTime);
        buffer.flip();

//        System.out.println("filename size: " + fileMetaData.filename.getBytes().length);

        int size = buffer.limit();
//        System.out.println("item size: " + size);

        byte[] bytes = new byte[size];

        // write FileMetaData from ByteBuffer into HDFS
        buffer.get(bytes);
        FSDataOutputStream fsDataOutputStream = fileSystem.append(path);
        fsDataOutputStream.write(bytes);
        fsDataOutputStream.close();

        countOfEntries++;
        sizeInBytes += buffer.limit();
    }

    /**
     * reconstruct a meta data RTree from meta data stored in HDFS
     */
    public static RTree<FileMetaData, Rectangle> reconstruct() throws IOException {

        Configuration conf = new Configuration();
        conf.set("dfs.replication", "1");
        conf.setBoolean("dfs.support.append", true);
        FileSystem fileSystem = null;
        try {
            fileSystem = FileSystem.get(new URI(TopologyConfig.HDFS_HOST_LOCAL), conf);
        } catch (URISyntaxException e) {
            e.printStackTrace();
        }
        Path path = new Path(TopologyConfig.HDFS_META_LOG_PATH);

        // check if the meta log file exist
        if (!fileSystem.exists(path)) throw new FileNotFoundException("Meta log file does not exist!");

        RTree<FileMetaData, Rectangle> newRTree = RTree.minChildren(minChildren).maxChildren(maxChildren).create();
        byte[] bytes = new byte[(int) sizeInBytes];

        // read all meta log from HDFS to ByteBuffer, need to revise to blocked reading mode.
        FSDataInputStream fsDataInputStream = fileSystem.open(path);
        fsDataInputStream.readFully(0, bytes);
        ByteBuffer buffer = ByteBuffer.wrap(bytes);

        // restore the byte data to FileMetaData format
        for (int i = 0; i < countOfEntries; i++) {

            // read filename from ByteBuffer
            int len = buffer.getInt();
            byte[] stringByte = new byte[len];
            buffer.get(stringByte);
            String fileName = new String(stringByte);

            // read 4 attributes from ByteBuffer
            double keyRangeLowerBound = buffer.getDouble();
            long startTime = buffer.getLong();
            double keyRangeUpperBound = buffer.getDouble();
            long endTime = buffer.getLong();

//            System.out.println("len: " + len + " fileName: " + fileName + " lower: " + keyRangeLowerBound
//             + " upper: " + keyRangeUpperBound + " start: " + startTime + " end: " + endTime);

            FileMetaData fileMetaData = new FileMetaData(fileName, keyRangeLowerBound, keyRangeUpperBound, startTime, endTime);

            // add one FileMetaData item to RTree
            newRTree = newRTree.add(fileMetaData, Geometries.rectangle(fileMetaData.keyRangeLowerBound, fileMetaData.startTime,
                    fileMetaData.keyRangeUpperBound, fileMetaData.endTime));
        }

        return newRTree;
    }

    /**
     * clear the existing meta log files in HDFS (for debugging only)
     */
    public static void clear() throws IOException {
        Configuration conf = new Configuration();
        conf.set("dfs.replication", "1");
        conf.setBoolean("dfs.support.append", true);

        FileSystem fileSystem = null;
        try {
            fileSystem = FileSystem.get(new URI(TopologyConfig.HDFS_HOST_LOCAL), conf);
        } catch (URISyntaxException e) {
            e.printStackTrace();
        }
        Path path = new Path(TopologyConfig.HDFS_META_LOG_PATH);

        fileSystem.delete(path, false);
    }

    public static void visualize() {
        rTree.visualize(600, 600).save("target/original.png");
    }
}