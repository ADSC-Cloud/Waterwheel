package indexingTopology.filesystem;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * Created by acelzj on 6/16/17.
 */
public class LocalWritingHandler implements WritingHandler {

    private FileOutputStream fop;

    private String path;

    private File file;

    private boolean append;

    public LocalWritingHandler(String path, boolean append) {
        this.path = path;
        this.append = append;
    }

    @Override
    public void writeToFileSystem(byte[] bytes, String fileName) throws IOException {
        try {
            fop.write(bytes);
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }


    @Override
    public void openFile(String fileName) throws IOException {
        file = new File(path + "/" + fileName);

        if (!append) {
            file.createNewFile();
        } else {
            if (!file.exists()) {
                throw new FileNotFoundException();
            }
        }

        fop = new FileOutputStream(file, append);
    }

    @Override
    public void closeFile() throws IOException {
        fop.flush();
        fop.close();
    }
}
