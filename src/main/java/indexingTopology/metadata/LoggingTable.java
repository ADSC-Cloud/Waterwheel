package indexingTopology.metadata;

import indexingTopology.config.TopologyConfig;

import com.github.davidmoten.rtree.RTree;
import com.github.davidmoten.rtree.geometry.Geometries;
import com.github.davidmoten.rtree.geometry.Rectangle;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;

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
    private static final int minChildren = 2;
    private static final int maxChildren = 4;
    private static final int MIN_ENTRY_SIZE_IN_BYTES = 38;
    private static RTree<FileMetaData, Rectangle> rTree = RTree.minChildren(minChildren).maxChildren(maxChildren).create();

    private LoggingTable () {

    }

    /**
     * Get the RTree maintained by LoggingTable
     * @return a RTree object constructed by appending operations
     */
    public static RTree<FileMetaData, Rectangle> getRTree() {
        return rTree;
    }

    /**
     * append a meta data item to the meta log file. If the file does not exist,
     * create a new file defined in {@link TopologyConfig#HDFS_META_LOG_PATH}
     */
    public static synchronized void append(FileMetaData fileMetaData) throws IOException {

        Configuration conf = new Configuration();
        conf.set("dfs.replication", "1");
        conf.setBoolean("dfs.support.append", true);
        FileSystem fileSystem = null;
        try {
            fileSystem = FileSystem.get(new URI(TopologyConfig.HDFS_HOST_LOCAL), conf);
        } catch (URISyntaxException e) {
            e.printStackTrace();
        } catch (IOException e) {
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
        buffer.putChar('|');
        buffer.flip();

        int actualSize = buffer.limit();
        byte[] bytes = new byte[actualSize];

        // write FileMetaData from ByteBuffer into HDFS
        buffer.get(bytes);
        FSDataOutputStream fsDataOutputStream = null;
        try {
            fsDataOutputStream = fileSystem.append(path);
        } catch (IOException e) {
            e.printStackTrace();
        }

        fsDataOutputStream.write(bytes);
        fsDataOutputStream.close();
        fileSystem.close();

        // add it to RTree in the end
        rTree = rTree.add(fileMetaData, Geometries.rectangle(fileMetaData.keyRangeLowerBound, fileMetaData.startTime,
                fileMetaData.keyRangeUpperBound, fileMetaData.endTime));
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

        final FileStatus fileStatus = fileSystem.getFileStatus(path);
        long len = fileStatus.getLen();
//        System.out.println("length of file: " + len);
        ByteBuffer buffer = ByteBuffer.allocate((int)len);

        RTree<FileMetaData, Rectangle> newRTree = RTree.minChildren(minChildren).maxChildren(maxChildren).create();

        // read all meta log from HDFS to ByteBuffer, need to revise to blocked reading mode.
        FSDataInputStream fsDataInputStream = fileSystem.open(path);
        fsDataInputStream.read(buffer);
        buffer.flip();
        fsDataInputStream.close();

        // restore the byte data to FileMetaData format
        while (buffer.remaining() >= MIN_ENTRY_SIZE_IN_BYTES){

            // read filename from ByteBuffer
            int fileNameLen = buffer.getInt();
            byte[] stringByte = new byte[fileNameLen];
            buffer.get(stringByte);
            String fileName = new String(stringByte);

            // read 4 attributes from ByteBuffer
            double keyRangeLowerBound = buffer.getDouble();
            long startTime = buffer.getLong();
            double keyRangeUpperBound = buffer.getDouble();
            long endTime = buffer.getLong();
            buffer.getChar();

//            System.out.println("len: " + len + " fileName: " + fileName + " lower: " + keyRangeLowerBound
//             + " upper: " + keyRangeUpperBound + " start: " + startTime + " end: " + endTime);

            FileMetaData fileMetaData = new FileMetaData(fileName, keyRangeLowerBound, keyRangeUpperBound, startTime, endTime);

            // add one FileMetaData item to RTree
            newRTree = newRTree.add(fileMetaData, Geometries.rectangle(fileMetaData.keyRangeLowerBound, fileMetaData.startTime,
                    fileMetaData.keyRangeUpperBound, fileMetaData.endTime));
        }
        fileSystem.close();
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
        fileSystem.close();
    }

    public static void visualize() {
        rTree.visualize(600, 600).save("target/originalTree.png");
    }
}