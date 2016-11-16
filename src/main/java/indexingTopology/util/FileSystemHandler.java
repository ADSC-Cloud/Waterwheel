package indexingTopology.util;

import java.io.IOException;

/**
 * Created by acelzj on 16-10-31.
 */
public interface FileSystemHandler {

    void writeToFileSystem(MemChunk chunk, String relativePath, String fileName) throws IOException;

    void createNewFile(String relativePath, String fileName);

    void openFile(String relativePath, String fileName);

    void readBytesFromFile(byte[] bytes);

    void seek(int offset) throws IOException;

    void closeFile();

    long getLengthOfFile(String relativePath, String fileName);
}
