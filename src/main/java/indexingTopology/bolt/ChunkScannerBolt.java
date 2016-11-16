package indexingTopology.bolt;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import indexingTopology.Config.Config;
import indexingTopology.DataSchema;
import indexingTopology.NormalDistributionIndexingTopology;
import indexingTopology.util.*;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Map;

/**
 * Created by acelzj on 11/9/16.
 */
public class ChunkScannerBolt extends BaseRichBolt {

    OutputCollector collector;

    private int bTreeOder;

    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        collector = outputCollector;
        bTreeOder = 4;
    }

    public void execute(Tuple tuple) {
        Double key = tuple.getDouble(0);
        ArrayList<String> fileNames = (ArrayList) tuple.getValue(1);
//        RandomAccessFile file = null;
        for (String fileName : fileNames) {
            try {
                FileSystemHandler fileSystemHandler = new LocalFileSystemHandler("/home/acelzj");
                fileSystemHandler.openFile("/", fileName);
                byte[] serializedTree = new byte[Config.TEMPLATE_SIZE];
                DeserializationHelper deserializationHelper = new DeserializationHelper();
                BytesCounter counter = new BytesCounter();

                fileSystemHandler.readBytesFromFile(serializedTree);
                BTree deserializedTree = deserializationHelper.deserializeBTree(serializedTree, bTreeOder, counter);
                int offset = deserializedTree.getOffsetOfLeaveNodeShouldContainKey(key);

                byte[] lengthInByte = new byte[4];
                fileSystemHandler.seek(offset);
                fileSystemHandler.readBytesFromFile(lengthInByte);
                int lengthOfLeaveInBytes = ByteBuffer.wrap(lengthInByte, 0, 4).getInt();

                byte[] leafInByte = new byte[lengthOfLeaveInBytes];
                fileSystemHandler.seek(offset + 4);
                fileSystemHandler.readBytesFromFile(leafInByte);
                BTreeLeafNode deserializedLeaf = deserializationHelper.deserializeLeaf(leafInByte, bTreeOder, counter);
                ArrayList<byte[]> serializedTuples = deserializedLeaf.searchAndGetTuples(key);
                if (serializedTuples != null) {
                    collector.emit(NormalDistributionIndexingTopology.FileSystemQueryStream,
                            new Values(key, serializedTuples));
                }
                fileSystemHandler.closeFile();


                /*
                file = new RandomAccessFile("/home/acelzj/" + fileName, "r");
                byte[] serializedTree = new byte[Config.TEMPLATE_SIZE];
                DeserializationHelper deserializationHelper = new DeserializationHelper();
                BytesCounter counter = new BytesCounter();

                file.read(serializedTree, 0, Config.TEMPLATE_SIZE);
                BTree deserializedTree = deserializationHelper.deserializeBTree(serializedTree, bTreeOder, counter);
                int offset = deserializedTree.getOffsetOfLeaveNodeShouldContainKey(key);
//                System.out.println("offset " + offset);
                byte[] lengthInByte = new byte[4];
                file.seek(offset);
                file.read(lengthInByte);
                int lengthOfLeaveInBytes = ByteBuffer.wrap(lengthInByte, 0, 4).getInt();
//                System.out.println("Length of leave in bytes " + lengthOfLeaveInBytes);
                byte[] leafInByte = new byte[lengthOfLeaveInBytes];
                file.seek(offset + 4);
                file.read(leafInByte);
                BTreeLeafNode deserializedLeaf = deserializationHelper.deserializeLeaf(leafInByte, bTreeOder, counter);
//                System.out.println("The key is " + key);
//                System.out.println("The leaf is ");
//                deserializedLeaf.print();
                ArrayList<byte[]> serializedTuples = deserializedLeaf.searchAndGetTuples(key);
                if (serializedTuples != null) {
                    collector.emit(NormalDistributionIndexingTopology.FileSystemQueryStream,
                            new Values(key, serializedTuples));
                }
                file.close();*/
            } catch (FileNotFoundException e) {
                e.printStackTrace();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        collector.ack(tuple);
    }

    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declareStream(NormalDistributionIndexingTopology.FileSystemQueryStream,
                new Fields("key", "serializedTuples"));
    }
}
