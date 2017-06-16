package indexingTopology.filesystem;

import java.io.IOException;

/**
 * Created by acelzj on 6/16/17.
 */
public interface WritingHandler {

    void writeToFileSystem(byte[] bytesToWrite, String fileName) throws IOException;

    void openFile(String fileName) throws IOException;

    void closeFile() throws IOException;
}
