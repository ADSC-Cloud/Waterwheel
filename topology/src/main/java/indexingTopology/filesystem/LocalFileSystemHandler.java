package indexingTopology.filesystem;

import indexingTopology.config.TopologyConfig;
import indexingTopology.common.MemChunk;

import java.io.*;
import java.nio.ByteBuffer;
import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * Created by dmir on 10/26/16.
 */
public class LocalFileSystemHandler implements FileSystemHandler {

    File file;
    FileOutputStream fop;
    String path;
    RandomAccessFile randomAccessFile;
    TopologyConfig config;

    public LocalFileSystemHandler(String path, TopologyConfig config) {
        this.path = path;
        this.config = config;
    }

    public void removeOldData(String filePathAndName) throws InterruptedException {
        file = new File(filePathAndName);
        if(file.delete()){
//            System.out.println("---------------- Delete file : "+ filePathAndName + "----------------");
        }
        else{
//            System.out.println("---------------- Can't find !" + "----------------");
        }
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
            fop.flush();
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

    public void openFile(String relativePath, String fileName) {
        try {
            randomAccessFile = new RandomAccessFile(path + relativePath + fileName, "r");
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
    }

    public void readBytesFromFile(int position, byte[] bytes) {
        try {
            randomAccessFile.seek(position);
            randomAccessFile.read(bytes);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void readBytesFromFile(byte[] bytes) {
        try {
            randomAccessFile.read(bytes);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void seek(int offset) {
        try {
            randomAccessFile.seek(offset);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void closeFile() {
        try {
            if (randomAccessFile != null)
                randomAccessFile.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

}
