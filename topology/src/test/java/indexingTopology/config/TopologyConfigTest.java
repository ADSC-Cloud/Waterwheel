package indexingTopology.config;

import junit.framework.TestCase;
import org.junit.Test;

import java.io.IOException;
import java.io.PrintWriter;

import static org.junit.Assert.*;

/**
 * Created by robert on 25/7/17.
 */
public class TopologyConfigTest extends TestCase {


    public void setUp() {
            try {
                PrintWriter out = new PrintWriter("conf.yaml.for-test");
                out.append(" storage.datachunk.folder: \"/datachunkfolder\"\n");
                out.append(" metadata.local.storage.path: \"/storagepath\"\n");
                out.append(" hdfs.host: \"/hdfshost\"\n");
                out.close();
            } catch (IOException e) {
                e.printStackTrace();
            }

    }

    public void tearDown() {
            try {
                Runtime.getRuntime().exec("rm conf.yaml.for-test");
            } catch (IOException e) {
                e.printStackTrace();
            }
    }

    @Test
    public void testOverride() throws Exception {
        TopologyConfig config = new TopologyConfig();
        config.override("conf.yaml.for-test");
        assertEquals("/hdfshost", config.HDFS_HOST);
        assertEquals("/datachunkfolder", config.dataChunkDir);
        assertEquals("/storagepath", config.metadataDir);
    }

}