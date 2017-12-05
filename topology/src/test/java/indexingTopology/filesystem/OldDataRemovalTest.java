package indexingTopology.filesystem;

import indexingTopology.bolt.MetadataServerBolt;
import indexingTopology.common.data.DataSchema;
import indexingTopology.config.TopologyConfig;
import org.junit.Test;

import java.io.File;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

import static org.junit.Assert.*;

public class OldDataRemovalTest {

    @Test
    public void removeExactlyOnce() throws Exception {
        LocalFileSystemHandler localFileSystemHandler = new LocalFileSystemHandler("", new TopologyConfig());
        File folder = new File("../data");
        if (folder.exists()) {
            File[] files = folder.listFiles();
            if (files.length == 0) {
                System.out.println("Folder empty");
                return;
            }
            else {
                System.out.println(files.length);
                for (File singleFile : files) {
                    System.out.println(singleFile.getPath());
                    if (singleFile.getName().equals(".DS_Store")) {
                                    continue;
                    }
//                    String ctime = new SimpleDateFormat("yyy-MM-dd hh:mm:ss").format(new Date(singleFile.lastModified()));
//                    String ntime = new SimpleDateFormat("yyy-MM-dd hh:mm:ss").format(new Date(System.currentTimeMillis()));
//                    System.out.println(ctime);
//                    System.out.println(ntime);
                    if (System.currentTimeMillis() - singleFile.lastModified() >= 20) {
                        try {
                            localFileSystemHandler.removeOldData(singleFile.getPath());
                        } catch (InterruptedException e) {
                            e.printStackTrace();
                        }
                    }
                }
            }
        }
        else{
            System.out.println("Can not find Folders");
        }
    }

    // Make sure that the file path add the "../"
    // The path is different from the MetadataServerbolt relative path
    @Test
    public void Localremove() throws Exception {
        DataSchema schema = new DataSchema();
        MetadataServerBolt metadataServerBolt = new MetadataServerBolt(0,10000,schema,new TopologyConfig());
        metadataServerBolt.startTimer(100,100);
    }

}