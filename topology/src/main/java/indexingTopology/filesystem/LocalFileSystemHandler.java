package indexingTopology.filesystem;

import indexingTopology.config.TopologyConfig;
import indexingTopology.util.MemChunk;

import java.io.*;
import java.nio.ByteBuffer;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

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

    public void writeToFileSystem(MemChunk chunk, String relativePath, String fileName) throws IOException {

        ExecutorService executorService = Executors.newSingleThreadExecutor();

        Integer waitingTimeInMilliSecond = 15 * 1000;


        createNewFile(relativePath, fileName);

        ByteBuffer buffer = chunk.getData();
        int size = chunk.getAllocatedSize();
        byte[] bytes = new byte[size];
        buffer.position(0);
        buffer.get(bytes);

//        try {
//            fop = new FileOutputStream(file);
//            ByteBuffer buffer = chunk.getData();
//            int size = chunk.getAllocatedSize();
//            byte[] bytes = new byte[size];
//            buffer.position(0);
//            buffer.get(bytes);
//            fop.write(bytes);
//        } catch (FileNotFoundException e) {
//            e.printStackTrace();
//        } finally {
//            fop.flush();
//            fop.close();
//        }

        Runnable writingTask = new Runnable() {
            @Override
            public void run() {

                try {
                    fop = new FileOutputStream(file);
//            ByteBuffer buffer = chunk.getData();
//            int size = chunk.getAllocatedSize();
//            byte[] bytes = new byte[size];
//            buffer.position(0);
//            buffer.get(bytes);
                    fop.write(bytes);
//                    fop.flush();
//                    fop.close();
                } catch (FileNotFoundException e) {
                    e.printStackTrace();
                } catch (IOException e) {
                    e.printStackTrace();
                } finally {

                }
            }
        };

        Future future = executorService.submit(writingTask);

        Integer waitingTime = 0;

        int retries = 0;

        int maxRetries = 5;
        while (retries < maxRetries) {
            while (waitingTime < waitingTimeInMilliSecond) {
                if (future.isDone()) {
                    fop.flush();
                    fop.close();
                    executorService.shutdown();
                    break;
                } else {
                    try {
                        Thread.sleep(10);
                        waitingTime += 10;
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }
            }

            if (!future.isDone()) {
                future.cancel(true);
                fop.close();
                executorService.submit(writingTask);
                ++retries;
                waitingTime = 0;
            } else {
                executorService.shutdown();
                fop.flush();
                fop.close();
                break;
            }
        }

        if (retries == maxRetries) {
            System.err.println("Writing tries exceed max retries");
            throw new RuntimeException();
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
            randomAccessFile.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

}
