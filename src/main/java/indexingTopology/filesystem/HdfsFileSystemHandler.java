package indexingTopology.filesystem;

import indexingTopology.config.TopologyConfig;
import indexingTopology.util.MemChunk;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.EOFException;
import java.io.IOException;
import java.net.URI;
import java.nio.ByteBuffer;

/**
 * Created by dmir on 10/26/16.
 */
public class HdfsFileSystemHandler implements FileSystemHandler {

    FileSystem fileSystem;
    URI uri;
    Configuration configuration;
    String path;
    FSDataInputStream fsDataInputStream;

    public HdfsFileSystemHandler(String path) throws IOException {
        configuration = new Configuration();
        configuration.setBoolean("dfs.support.append", true);
        uri = URI.create(TopologyConfig.HDFS_HOST + path);
        fileSystem = FileSystem.get(uri, configuration);
        this.path = path;
    }

    public void writeToFileSystem(MemChunk chunk, String relativePath, String fileName) throws IOException{

        createNewFile(relativePath, fileName);

        ByteBuffer buffer = chunk.getData();
        int size = chunk.getAllocatedSize();
        byte[] bytes = new byte[size];
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

    public void openFile(String relativePath, String fileName) {
        try {
            fsDataInputStream = fileSystem.open(new Path(this.path + relativePath + fileName));
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void readBytesFromFile(int position, byte[] bytes) {
        try {
            fsDataInputStream.readFully(position, bytes);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }


    public void readBytesFromFile(byte[] bytes) {
            readBytesFromFile(0, bytes);
    }


    public void seek(int offset) throws IOException {
        try {
            fsDataInputStream.seek(offset);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void closeFile() {
        try {
            fsDataInputStream.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public FileSystem getFileSystem() {
        return fileSystem;
    }
}
