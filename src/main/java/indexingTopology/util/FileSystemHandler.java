package indexingTopology.util;

import java.io.IOException;

/**
 * Created by acelzj on 16-10-31.
 */
public interface FileSystemHandler {

    void writeToFileSystem(MemChunk chunk, String relativePath, String fileName) throws IOException;
    void createNewFile(String relativePath, String fileName);

}
