package indexingTopology.util;

import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.nio.file.FileStore;
import java.nio.file.FileSystems;
import java.nio.file.Files;

import com.sun.management.OperatingSystemMXBean;

/**
 * Created by robert on 23/8/17.
 */
public class MonitorUtils {
    public static double getProcessCpuLoad() {
        OperatingSystemMXBean operatingSystemMXBean =
                (com.sun.management.OperatingSystemMXBean)ManagementFactory.getOperatingSystemMXBean();
        return operatingSystemMXBean.getSystemCpuLoad();
    }

    public static double getFreeDiskSpaceInGB(String path) {
        String filePath;
        if (path == null)
            filePath = "/";
        else
            filePath = path;
        try {
            FileStore store = Files.getFileStore(FileSystems.getDefault().getPath(filePath, "/"));
            return store.getUsableSpace() / 1024.0 / 1024 / 1024;
        } catch (IOException e) {
            e.printStackTrace();
            return 0;
        }
    }

    public static double getTotalDiskSpaceInGB(String path) {
        String filePath;
        if (path == null)
            filePath = "/";
        else
            filePath = path;
        try {
            FileStore store = Files.getFileStore(FileSystems.getDefault().getPath(filePath, "/"));
            return store.getTotalSpace() / 1024.0 / 1024 / 1024;
        } catch (IOException e) {
            e.printStackTrace();
            return 0;
        }
    }

    static public void main(String[] args) throws InterruptedException {
        while (true) {
            Thread.sleep(1000);
            System.out.println(getProcessCpuLoad());
            System.out.println(getFreeDiskSpaceInGB(null) + "GB");
            System.out.println(getTotalDiskSpaceInGB(null) + "GB");
        }
    }
}
