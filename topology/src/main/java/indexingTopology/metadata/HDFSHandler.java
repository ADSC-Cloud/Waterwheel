package indexingTopology.metadata;

import com.github.davidmoten.rtree.RTree;
import com.github.davidmoten.rtree.geometry.Geometries;
import com.github.davidmoten.rtree.geometry.Rectangle;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.storm.hdfs.bolt.HdfsBolt;
import org.apache.storm.hdfs.bolt.format.DefaultFileNameFormat;
import org.apache.storm.hdfs.bolt.format.DelimitedRecordFormat;
import org.apache.storm.hdfs.bolt.format.FileNameFormat;
import org.apache.storm.hdfs.bolt.format.RecordFormat;
import org.apache.storm.hdfs.bolt.rotation.FileRotationPolicy;
import org.apache.storm.hdfs.bolt.rotation.NoRotationPolicy;
import org.apache.storm.hdfs.bolt.sync.CountSyncPolicy;
import org.apache.storm.hdfs.bolt.sync.SyncPolicy;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Random;
import java.util.Scanner;

public class HDFSHandler {
    private static String default_stream_name = "hdfs-stream";
    private static final int minChildren = 2;
    private static final int maxChildren = 4;

    private static long timeCursor = 0;

    /**
     * generate a HdfsBolt that is responsible for writing meta logs into HDFS
     * @return a HdfsBolt configured by default parameters
     */
    public static HdfsBolt getHdfsWritterBolt() {
        SyncPolicy syncPolicy = new CountSyncPolicy(10);
        FileRotationPolicy fileRotationPolicy = new NoRotationPolicy();
        FileNameFormat fileNameFormat = new DefaultFileNameFormat().withPath(HDFSBoltConfig.HDFS_META_LOG_RELATIVE_PATH);

        // default Field delimiter: ',', default Record delimiter: '\n', i.e., one line for one record in HDFS
        RecordFormat recordFormat = new DelimitedRecordFormat();
        HdfsBolt hdfsWritterBolt = new HdfsBolt()
                .withFsUrl(HDFSBoltConfig.HDFS_LOCAL_HOST)
                .withFileNameFormat(fileNameFormat)
                .withRecordFormat(recordFormat)
                .withRotationPolicy(fileRotationPolicy)
                .withSyncPolicy(syncPolicy);

        return hdfsWritterBolt;
    }

    public static String getFromStream() {
        return default_stream_name;
    }

    public static String emitToStream() {
        return default_stream_name;
    }

    public static Fields setFields() {
        return new Fields("filename", "lowerbound", "upperbound", "starttime", "endtime");
    }

    public static Values setValues(FileMetaData fileMetaData) {
        return new Values(fileMetaData.filename, fileMetaData.keyRangeLowerBound, fileMetaData.keyRangeUpperBound,
                fileMetaData.startTime, fileMetaData.endTime);
    }

    /**
     * reconstruct a RTree from data stored in HDFS
     * @throws IOException
     */
    public static RTree<FileMetaData, Rectangle> reconstructRTree() throws IOException {

        RTree<FileMetaData, Rectangle> rTree = RTree.minChildren(minChildren).maxChildren(maxChildren).create();

        final Path dirPath = new Path(HDFSBoltConfig.HDFS_LOCAL_HOST + HDFSBoltConfig.HDFS_META_LOG_RELATIVE_PATH);
        final Configuration conf = new Configuration();
        conf.set("dfs.replication", "1");
        conf.setBoolean("dfs.support.append", true);
        FileSystem fs = null;
        try {
            fs = FileSystem.get(new URI(HDFSBoltConfig.HDFS_LOCAL_HOST), conf);
        } catch (URISyntaxException e) {
            e.printStackTrace();
        }

        final FileStatus[] fileStatus = fs.listStatus(dirPath);

        int count = 0;
        for (int i = 0; i < fileStatus.length; i++) {
            long len = fileStatus[i].getLen();
            System.out.println("length of this file: " + len);
            FSDataInputStream in = fs.open(fileStatus[i].getPath());
            Scanner scanner = new Scanner(in);

            // read one record each time
            while(scanner.hasNextLine()) {
                String line = scanner.nextLine();
                String[] fields = line.split(",");

                // parse the text data from HDFS to original format
                String fileName = fields[0];
                double keyRangeLowerBound = Double.parseDouble(new BigDecimal(fields[1]).toPlainString());
                double keyRangeUpperBound = Double.parseDouble(new BigDecimal(fields[2]).toPlainString());
                long startTime = Long.parseLong(new BigInteger(fields[3]).toString());
                long endTime = Long.parseLong(new BigInteger(fields[4]).toString());

                // add one FileMetaData item to RTree
                FileMetaData fileMetaData = new FileMetaData(fileName, keyRangeLowerBound, keyRangeUpperBound, startTime, endTime);
                rTree = rTree.add(fileMetaData, Geometries.rectangle(fileMetaData.keyRangeLowerBound, fileMetaData.startTime,
                        fileMetaData.keyRangeUpperBound, fileMetaData.endTime));
                count++;
            }
            in.close();
            System.out.println("Number of entries added: " + count);
        }
        fs.close();
        return rTree;
    }

    public static FileMetaData generateFileMetaData() {
        Random rand = new Random();
        String fileName = "DefaultFileName";

        double keyRangeLowerBound = rand.nextDouble() * 600;
        double keyRangeUpperBound = keyRangeLowerBound + rand.nextDouble() * 2.0 + 2.0;
//        long startTime = rand.nextInt(4);
//        long endTime = startTime + rand.nextInt(4);
        long startTime = timeCursor;
        long endTime = startTime + rand.nextInt(2) + 2;
        timeCursor = endTime;

        return new FileMetaData(fileName, keyRangeLowerBound, keyRangeUpperBound, startTime, endTime);
    }

    /**
     * delete the existing meta log files in HDFS
     * @throws IOException
     */
    public static void clearExistingLogs() throws IOException {
        final Path dirPath = new Path(HDFSBoltConfig.HDFS_LOCAL_HOST + HDFSBoltConfig.HDFS_META_LOG_RELATIVE_PATH);
        final Configuration conf = new Configuration();
        conf.set("dfs.replication", "1");
        conf.setBoolean("dfs.support.append", true);

        FileSystem fs = null;
        try {
            fs = FileSystem.get(new URI(HDFSBoltConfig.HDFS_LOCAL_HOST), conf);
        } catch (URISyntaxException e) {
            e.printStackTrace();
        }

        final FileStatus[] fileStatus = fs.listStatus(dirPath);
        for (int i = 0; i < fileStatus.length; i++) {
            fs.delete(fileStatus[i].getPath(), true);
        }
        fs.close();
    }

    /**
     * generate a PNG image for a given RTree
     * @param rTree
     */
    public static void visualizeRTree(RTree<FileMetaData, Rectangle> rTree) {
        rTree.visualize(600, 600).save("target/RTree.png");
    }
}
