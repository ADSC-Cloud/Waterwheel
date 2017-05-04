package indexingTopology;

import indexingTopology.bolt.Bolt1;
import indexingTopology.bolt.Bolt2;
import indexingTopology.bolt.Bolt3;

import com.github.davidmoten.rtree.RTree;
import com.github.davidmoten.rtree.geometry.Rectangle;
import indexingTopology.metadata.FileMetaData;
import indexingTopology.metadata.HDFSBoltConfig;
import indexingTopology.metadata.HDFSHandler;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.utils.Utils;
import indexingTopology.spout.Spout;

/**
 * Test the HDFSHandler class
 */
public class StormHDFSTest {

    private static final boolean LOCAL_MODE = true;

    public static void main(String[] args) throws Exception {
        Config config = new Config();
        config.setNumWorkers(4);
        config.setDebug(true);

        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("spout", new Spout(), 1);
        builder.setBolt("bolt1", new Bolt1(), 4).shuffleGrouping("spout");
        builder.setBolt("bolt2", new Bolt2(), 4).shuffleGrouping("spout");
        builder.setBolt("bolt3", new Bolt3(), 4).shuffleGrouping("bolt1").shuffleGrouping("bolt2");

        // set HDFS bolt
        builder.setBolt("hdfs-bolt", HDFSHandler.getHdfsWritterBolt(), 4).
                shuffleGrouping("bolt1", HDFSHandler.getFromStream()).
                shuffleGrouping("bolt2", HDFSHandler.getFromStream());

        HDFSHandler.clearExistingLogs();
        if (LOCAL_MODE) {
            LocalCluster localCluster = new LocalCluster();
            localCluster.submitTopology("hdfs-topology", config, builder.createTopology());
            Utils.sleep(10000);
            localCluster.killTopology("hdfs-topology");
            localCluster.shutdown();

            // reconstruct and visualize RTree
            if (HDFSBoltConfig.RECONSTRUCT) {
                RTree<FileMetaData, Rectangle> rTree = HDFSHandler.reconstructRTree();
                HDFSHandler.visualizeRTree(rTree);
                System.out.println("RTree size: " + rTree.size());
            }
        }
        else {
            StormSubmitter.submitTopology("hdfs-topology", config, builder.createTopology());
        }
    }
}
