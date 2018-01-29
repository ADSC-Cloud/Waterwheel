package indexingTopology.filesystem;

import indexingTopology.bolt.MetadataServerBolt;
import indexingTopology.common.data.DataSchema;
import indexingTopology.config.TopologyConfig;
import indexingTopology.metadata.FileMetaData;
import indexingTopology.metadata.FilePartitionSchemaManager;
import org.junit.Test;

import java.io.File;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.List;

import static org.junit.Assert.*;

public class OldDataRemovalTest {

    // Make sure that the file path add the "../"
    // The path is different from the MetadataServerbolt relative path
    @Test
    public void removeExactlyOnce() throws Exception {
        TopologyConfig config = new TopologyConfig();
        LocalFileSystemHandler localFileSystemHandler = new LocalFileSystemHandler("", config);
        config.dataChunkDir = "data";
        File folder = new File("../" + config.dataChunkDir);
        int previousTime = 20;
        if (folder.exists()) {
            File[] files = folder.listFiles();
            if (files.length == 0) {
                return;
            }
            else {
                System.out.println(files.length);
                for (File singleFile : files) {
                    System.out.println(singleFile.getPath());
                    if (singleFile.getName().equals(".DS_Store")) {
                                    continue;
                    }

                    if (System.currentTimeMillis() - singleFile.lastModified() >= previousTime * 1000) {
                        try {
                            localFileSystemHandler.removeOldData(singleFile.getPath());
                        } catch (InterruptedException e) {
                            e.printStackTrace();
                        }
                    }
                }
            }
        }
    }

}