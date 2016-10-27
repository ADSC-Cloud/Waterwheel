package indexingTopology.util;

import indexingTopology.Config.Config;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.net.URI;
import java.nio.ByteBuffer;

/**
 * Created by dmir on 10/26/16.
 */
public class HdfsFileSystemHandler implements FileSystemHandler{

    FileSystem fileSystem;
    URI uri;
    Configuration configuration;
    String path;
    public HdfsFileSystemHandler(String path) throws IOException {
        configuration = new Configuration();
        configuration.setBoolean("dfs.support.append", true);
        uri = URI.create(Config.HDFS_HOST + path);
        fileSystem = FileSystem.get(uri, configuration);
        this.path = path;
    }

    public void writeToFileSystem(MemChunk chunk, String relativePath, String fileName) throws IOException{
        ByteBuffer buffer = chunk.getData();
        int offset = chunk.getOffset();
        byte[] bytes = new byte[offset];
        buffer.position(0);
        buffer.get(bytes);
        Path path = new Path(this.path + relativePath + fileName);
//        FSDataOutputStream fsDataOutputStream = null;
//        try {
//            fsDataOutputStream = fileSystem.create(path);
//        } catch (IOException e) {
//            e.printStackTrace();
//        }
        FSDataOutputStream fsDataOutputStream = fileSystem.append(path);
        fsDataOutputStream.write(bytes);
        fsDataOutputStream.close();
    }

    public void createNewFile(String relativePath, String fileName) {
        Path path = new Path(this.path + relativePath + fileName);
        try {
            fileSystem.create(path).close();
        } catch (IOException e) {
            e.printStackTrace();
        }

    }
}
