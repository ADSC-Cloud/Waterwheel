package indexingTopology.filesystem;

import java.io.FileNotFoundException;
import java.io.IOException;

/**
 * Created by acelzj on 6/16/17.
 */
public interface ReadingHandler {

    void openFile(String fileName) throws IOException;

    void closeFile() throws IOException;

    int read(byte[] buffer) throws IOException;

    long getFileLength() throws IOException;
}
