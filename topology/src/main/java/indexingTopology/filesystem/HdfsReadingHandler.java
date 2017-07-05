package indexingTopology.filesystem;

import indexingTopology.config.TopologyConfig;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.net.URI;

/**
 * Created by acelzj on 6/16/17.
 */
public class HdfsReadingHandler implements ReadingHandler {
    private final Configuration configuration;
    private final URI uri;
    private final Path path;

    private FileSystem fileSystem;
    private FSDataInputStream fsDataInputStream;
    private String fileName;

    public HdfsReadingHandler(String path, TopologyConfig config) {
        this.path = new Path(path);
        this.configuration = new Configuration();
        this.configuration.setBoolean("dfs.support.append", true);
        this.configuration.setBoolean("dfs.api.read.shortcircuit", true);
        this.configuration.set("dfs.domain.socket.path", "/var/lib/hadoop-hdfs/dn_socket");
        this.uri = URI.create(config.HDFS_HOST + path);
    }

    @Override
    public void openFile(String fileName) throws IOException {
        this.fileName = fileName;

        fileSystem = FileSystem.get(uri, configuration);
        fsDataInputStream = fileSystem.open(new Path(this.path + "/" + fileName));
    }

    @Override
    public void closeFile() throws IOException {
        fsDataInputStream.close();
    }

    @Override
    public int read(byte[] buffer) throws IOException {
        return fsDataInputStream.read(0, buffer, 0, buffer.length);
    }

    @Override
    public long getFileLength() throws IOException {
        return fileSystem.getFileStatus(new Path(path, fileName)).getLen();
    }
}
