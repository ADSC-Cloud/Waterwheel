package indexingTopology.filesystem;

import indexingTopology.config.TopologyConfig;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;

import java.io.IOException;
import java.net.URI;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

/**
 * Created by acelzj on 6/16/17.
 */
public class HdfsWritingHandler implements WritingHandler {

    private FileSystem fileSystem;
    private URI uri;
    private Configuration configuration;
    private String path;
    private FSDataOutputStream fsDataOutputStream;
    private boolean append;

    public HdfsWritingHandler(TopologyConfig config, String path, boolean append) {
        this.path = path;
        this.append = append;
        configuration = new Configuration();
        configuration.setBoolean("dfs.support.append", true);
        configuration.setBoolean("dfs.api.read.shortcircuit", true);
        configuration.set("dfs.domain.socket.path", "/var/lib/hadoop-hdfs/dn_socket");
        uri = URI.create(config.HDFS_HOST + path);
    }

    @Override
    public void writeToFileSystem(byte[] bytes, String fileName) throws IOException {
        ExecutorService executorService = Executors.newSingleThreadExecutor();

        Integer waitingTimeInMilliSecond = 15 * 1000;

        Runnable writingTask = new Runnable() {
            @Override
            public void run() {
                try {
                    fsDataOutputStream.write(bytes);
                } catch (IOException e) {
                    e.printStackTrace();
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
                executorService.submit(writingTask);
                ++retries;
                waitingTime = 0;
            } else {
                executorService.shutdown();
                break;
            }
        }

        if (retries == maxRetries) {
            System.err.println("Writing tries exceed max retries");
            throw new RuntimeException();
        }
    }

    @Override
    public void openFile(String fileName) throws IOException {
        Path path = new Path(this.path + "/" + fileName);
        fileSystem = FileSystem.get(uri, configuration);

        if (!append) {
            fsDataOutputStream = fileSystem.create(path, true);
        } else {
            if (fileSystem.exists(path)) {
                throw new PathNotFoundException("File can't be append in this path");
            }
            fsDataOutputStream = fileSystem.append(path);
        }
    }

    @Override
    public void closeFile() throws IOException {
        fsDataOutputStream.close();
    }
}
