package indexingTopology.filesystem;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;

/**
 * Created by acelzj on 6/16/17.
 */
public class LocalReadingHandler implements ReadingHandler {

    private RandomAccessFile randomAccessFile;
    private final String path;

    public LocalReadingHandler(String path) {
        this.path = path;
    }

    @Override
    public void openFile(String fileName) throws FileNotFoundException {
        randomAccessFile = new RandomAccessFile(path + "/" + fileName, "r");
    }

    @Override
    public void closeFile() throws IOException {
        randomAccessFile.close();
    }

    @Override
    public int read(byte[] buffer) throws IOException {
        return randomAccessFile.read(buffer);
    }

    @Override
    public long getFileLength() {
        try {
            return randomAccessFile.length();
        } catch (IOException e) {
            e.printStackTrace();
        }

        return 0;
    }
}
