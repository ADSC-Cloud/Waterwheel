package indexingTopology.filesystem;

import indexingTopology.config.TopologyConfig;
import indexingTopology.util.MemChunk;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.net.URI;
import java.nio.ByteBuffer;
import java.util.concurrent.*;

/**
 * Created by dmir on 10/26/16.
 */
public class HdfsFileSystemHandler implements FileSystemHandler {

    FileSystem fileSystem;
    URI uri;
    Configuration configuration;
    String path;
    FSDataInputStream fsDataInputStream;

    TopologyConfig config;

    public HdfsFileSystemHandler(String path, TopologyConfig config) throws IOException {
        configuration = new Configuration();
        configuration.setBoolean("dfs.support.append", true);
        configuration.setBoolean("dfs.api.read.shortcircuit", true);
        configuration.set("dfs.domain.socket.path", "/var/lib/hadoop-hdfs/dn_socket");
        this.config = config;
        uri = URI.create(config.HDFS_HOST + path);
        fileSystem = FileSystem.get(uri, configuration);
        this.path = path;
    }

    public void writeToFileSystem(MemChunk chunk, String relativePath, String fileName) throws IOException{
        ExecutorService executorService = Executors.newSingleThreadExecutor();

        Integer waitingTimeInMilliSecond = 15 * 1000;

        createNewFile(relativePath, fileName);

        ByteBuffer buffer = chunk.getData();
        int size = chunk.getAllocatedSize();
        byte[] bytes = new byte[size];
        buffer.position(0);
        buffer.get(bytes);
        Path path = new Path(this.path + relativePath + fileName);
//        FSDataOutputStream fsDataOutputStream = null;
//        try {
//            fsDataOutputStream = fileSystem.create(path);
//        } catch (IOException e) {
//            e.printStackTrace();
//        }

        Callable<Boolean> writingTask = new Callable<Boolean>() {
            @Override
            public Boolean call() throws Exception {
                FSDataOutputStream fsDataOutputStream = null;
                try {
                    fsDataOutputStream = fileSystem.append(path);
                    fsDataOutputStream.write(bytes);
                    return true;
                } catch (IOException e) {
                    e.printStackTrace();
                } finally {
                    if (fsDataOutputStream != null)
                        try {
                            fsDataOutputStream.close();
                        } catch (IOException e) {
                            e.printStackTrace();
                        }
                }
                return false;
            }
        };

//        Runnable writingTask = new Runnable() {
//            @Override
//            public void run() {
//                FSDataOutputStream fsDataOutputStream = null;
//                try {
//                    fsDataOutputStream = fileSystem.append(path);
//                    fsDataOutputStream.write(bytes);
//                    fsDataOutputStream.close();
//                } catch (IOException e) {
//                    e.printStackTrace();
//                }
//            }
//        };

        Future<Boolean> future = executorService.submit(writingTask);

        Integer waitingTime = 0;

        int retries = 0;

        int maxRetries = 5;

        boolean success = false;
        while (retries <= maxRetries && !success) {

            try {
                success = future.get(15, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                e.printStackTrace();
                break;
            } catch (ExecutionException e) {
                e.printStackTrace();
            } catch (TimeoutException e) {
                e.printStackTrace();
            } catch (Exception e) {
                e.printStackTrace();
            }
            retries++;
        }
        executorService.shutdownNow();

//        while (retries < maxRetries) {
//            while (waitingTime < waitingTimeInMilliSecond) {
//                if (future.isDone()) {
//                    executorService.shutdown();
//                    break;
//                } else {
//                    try {
//                        Thread.sleep(10);
//                        waitingTime += 10;
//                    } catch (InterruptedException e) {
//                        e.printStackTrace();
//                    }
//                }
//            }
//
//            if (!future.isDone()) {
//                future.cancel(true);
//                executorService.submit(writingTask);
//                ++retries;
//                waitingTime = 0;
//            } else {
//                executorService.shutdown();
//                break;
//            }
//        }

        if (retries == maxRetries) {
            throw new IOException("Writing " + fileName + " to HDFS fails: writing retries exceed 5. ");
        }
//        FSDataOutputStream fsDataOutputStream = fileSystem.append(path);
//        fsDataOutputStream.write(bytes);
//        fsDataOutputStream.close();
    }

    public void createNewFile(String relativePath, String fileName) {
        Path path = new Path(this.path + relativePath + fileName);
        try {
            fileSystem.create(path).close();
        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    public void openFile(String relativePath, String fileName) {
        try {
            fsDataInputStream = fileSystem.open(new Path(this.path + relativePath + fileName));
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void readBytesFromFile(int position, byte[] bytes) {
        try {
            fsDataInputStream.readFully(position, bytes);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }


    public void readBytesFromFile(byte[] bytes) {
            readBytesFromFile(0, bytes);
    }


    public void seek(int offset) throws IOException {
        try {
            fsDataInputStream.seek(offset);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void closeFile() {
        try {
            fsDataInputStream.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public FileSystem getFileSystem() {
        return fileSystem;
    }
}
