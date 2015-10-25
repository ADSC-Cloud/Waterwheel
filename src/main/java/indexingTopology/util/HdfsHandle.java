package indexingTopology.util;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.util.Map;

/**
 * Created by parijatmazumdar on 24/10/15.
 */
public class HdfsHandle {
    private FileSystem hdfs;
    public HdfsHandle(Map confMap) throws IOException {
        Configuration conf = new Configuration();
        conf.addResource(new Path((String) confMap.get(Constants.HDFS_CORE_SITE.str)));
        conf.addResource(new Path((String) confMap.get(Constants.HDFS_HDFS_SITE.str)));
        hdfs = FileSystem.get(conf);
    }

    public void writeToNewFile(byte [] data, String relativeFilePath) throws IOException {
        Path newFile = new Path(hdfs.getUri()+"/"+relativeFilePath);
        FSDataOutputStream out = hdfs.create(newFile,false);
        out.write(data);
        out.close();
    }
}
