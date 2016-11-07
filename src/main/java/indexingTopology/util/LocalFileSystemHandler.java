package indexingTopology.util;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

/**
 * Created by dmir on 10/26/16.
 */
public class LocalFileSystemHandler implements FileSystemHandler{

    File file;
    FileOutputStream fop;
    String path;
    public LocalFileSystemHandler(String path) {
        this.path = path;
    }
    public void writeToFileSystem(MemChunk chunk, String relativePath, String fileName) throws IOException {

        createNewFile(relativePath, fileName);

        try {
            fop = new FileOutputStream(file);
            ByteBuffer buffer = chunk.getData();
            int size = chunk.getAllocatedSize();
            byte[] bytes = new byte[size];
            buffer.position(0);
            buffer.get(bytes);
            fop.write(bytes);
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } finally {
            fop.close();
        }
    }

    public void createNewFile(String relativePath, String fileName) {
        file = new File(path + relativePath + fileName);
        if (!file.exists()) {
            try {
                file.createNewFile();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }
}
