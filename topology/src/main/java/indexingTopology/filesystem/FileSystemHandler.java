package indexingTopology.filesystem;

import indexingTopology.util.MemChunk;

import java.io.IOException;

/**
 * Created by acelzj on 16-10-31.
 */
public interface FileSystemHandler {

    void writeToFileSystem(MemChunk chunk, String relativePath, String fileName) throws IOException;

    void createNewFile(String relativePath, String fileName);

    void openFile(String relativePath, String fileName);

    void readBytesFromFile(int position, byte[] bytes);

    void readBytesFromFile(byte[] bytes);

    void seek(int offset) throws IOException;

    void closeFile();

}
