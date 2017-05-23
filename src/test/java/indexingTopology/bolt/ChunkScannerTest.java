package indexingTopology.bolt;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import indexingTopology.data.DataSchema;
import indexingTopology.data.DataTuple;
import indexingTopology.cache.*;
import indexingTopology.config.TopologyConfig;
import indexingTopology.filesystem.FileSystemHandler;
import indexingTopology.filesystem.HdfsFileSystemHandler;
import indexingTopology.filesystem.LocalFileSystemHandler;
import indexingTopology.util.*;
import javafx.util.Pair;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;

/**
 * Created by acelzj on 23/2/17.
 */
public class ChunkScannerTest <TKey extends Comparable<TKey>> {

    private static DataSchema schema;

    private static ArrayBlockingQueue<String> queue;

    static TopologyConfig config = new TopologyConfig();

    public static void main(String[] args) throws IOException {

        System.out.println(Runtime.getRuntime().totalMemory() * 1.0 / 1024 / 1024);

        final int payloadSize = 10;
        schema = new DataSchema();
//        schema.addDoubleField("id");
        schema.addLongField("id");
        schema.addDoubleField("zcode");
//        schema.addIntField("zcode");
        schema.addVarcharField("payload", payloadSize);
        schema.setPrimaryIndexField("zcode");

        schema.addLongField("timestamp");


        queue = new ArrayBlockingQueue<String>(4);


        DataSchema schemaWithTimestamp = schema.duplicate();
        schemaWithTimestamp.addLongField("timestamp");

        List<String> fileNames = new ArrayList<>();
        fileNames.add("taskId9chunk6");
        fileNames.add("taskId9chunk7");
        fileNames.add("taskId9chunk8");
        fileNames.add("taskId9chunk9");

        Thread generateThread = new Thread(new Runnable() {
            @Override
            public void run() {
                int index = 0;
                while (true) {
                    try {
                        Thread.sleep(500);
                        queue.put(fileNames.get(index));
                        ++index;
                        if (index == fileNames.size()) {
                            index = 0;
                        }
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }
            }
        });
        generateThread.start();

        for (int i = 0; i < 1; ++i) {
            Thread scanThread = new Thread(new Runnable() {
                @Override
                public void run() {
                    while (true) {
                        try {
                            String fileName = queue.take();
//                            System.out.println(fileName);
//                            Long memoryBefore = Runtime.getRuntime().freeMemory();
                            long start = System.currentTimeMillis();
                            handleSubQuery(fileName);
//                            System.out.println(Thread.currentThread().getName() + " total " + (System.currentTimeMillis() - start));
                            System.out.println(Thread.currentThread().getName() + " file name " + fileName);
//                            System.out.println((Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory()) * 1.0 / 1024 / 1024);
                        } catch (InterruptedException e) {
                            e.printStackTrace();
                        } catch (IOException e) {
                            e.printStackTrace();
                        }
                    }
                }
            });
            scanThread.start();
        }

    }

        private static void handleSubQuery(String fileName) throws IOException {

            long start = System.currentTimeMillis();

            Kryo kryo = new Kryo();
            kryo.register(BTree.class, new KryoTemplateSerializer(config));
            kryo.register(BTreeLeafNode.class, new KryoLeafNodeSerializer(config));

            Double leftKey = 0.0;
            Double rightKey = 0.01;


            long fileTime = 0;
            long fileStart = System.currentTimeMillis();
            FileSystemHandler fileSystemHandler = null;
            if (config.HDFSFlag) {
                fileSystemHandler = new HdfsFileSystemHandler(config.dataDir, config);
            } else {
                fileSystemHandler = new LocalFileSystemHandler(config.dataDir, config);
            }
            fileSystemHandler.openFile("/", fileName);
            fileTime += (System.currentTimeMillis() - fileStart);

            long readTemplateStart = System.currentTimeMillis();
            Pair data = getTemplateData(fileSystemHandler, fileName);
            long temlateRead = System.currentTimeMillis() - readTemplateStart;

            BTree template = (BTree) data.getKey();
            Integer length = (Integer) data.getValue();


            BTreeLeafNode leaf = null;
            ArrayList<byte[]> tuples = new ArrayList<byte[]>();


            long searchOffsetsStart = System.currentTimeMillis();
            List<Integer> offsets = template.getOffsetsOfLeafNodesShouldContainKeys(leftKey, rightKey);
//        System.out.println("search offset " + (System.currentTimeMillis() - searchOffsetsStart));

            long readLeafBytesStart = System.currentTimeMillis();
            byte[] bytesToRead = readLeafBytesFromFile(fileSystemHandler, offsets, length);
            long leafBytesReadingTime = System.currentTimeMillis() - readLeafBytesStart;

            long totalTupleGet = 0L;
            long totalLeafRead = 0L;
            Input input = new Input(bytesToRead);
            for (Integer offset : offsets) {
                BlockId blockId = new BlockId(fileName, offset + length + 4);

                long readLeaveStart = System.currentTimeMillis();
//                leaf = (BTreeLeafNode) getFromCache(blockId);

                if (leaf == null) {
                    leaf = getLeafFromExternalStorage(fileSystemHandler, input);
                    CacheData cacheData = new LeafNodeCacheData(leaf);
//                    putCacheData(blockId, cacheData);
                }
                totalLeafRead += (System.currentTimeMillis() - readLeaveStart);

                long tupleGetStart = System.currentTimeMillis();
                ArrayList<byte[]> tuplesInKeyRange = leaf.getTuplesWithinKeyRange(leftKey, rightKey);
                ArrayList<byte[]> tuplesWithinTimestamp = getTuplesWithinTimestamp(tuplesInKeyRange, 0L, Long.MAX_VALUE);
                totalTupleGet += (System.currentTimeMillis() - tupleGetStart);

                if (tuplesWithinTimestamp.size() != 0) {
                    tuples.addAll(tuplesWithinTimestamp);
                }

            }

            long closeStart = System.currentTimeMillis();
            fileSystemHandler.closeFile();
            fileTime += (System.currentTimeMillis() - closeStart);


        System.out.println("total time " + (System.currentTimeMillis() - start));
        System.out.println("file open and close time " + fileTime);
        System.out.println("template read " + temlateRead);
        System.out.println("leaf read " + totalLeafRead);
        System.out.println("total tuple get" + totalTupleGet);
        System.out.println("leaf bytes reading time " + leafBytesReadingTime);


//            metrics.setTotalTime(System.currentTimeMillis() - start);

//        collector.emit(Streams.FileSystemQueryStream, new Values(queryId, tuples, metrics));
            tuples.clear();
//            collector.emit(Streams.FileSystemQueryStream, new Values(queryId, tuples, metrics));

//        collector.emit(Streams.FileSubQueryFinishStream, new Values("finished"));
        }


//    private Object getFromCache(BlockId blockId) {
//        if (blockIdToCacheUnit.get(blockId) == null) {
//            return null;
//        }
//        return blockIdToCacheUnit.get(blockId).getCacheData().getData();
//    }


//    private void putCacheData(BlockId blockId, CacheData cacheData) {
//        CacheUnit cacheUnit = new CacheUnit();
//        cacheUnit.setCacheData(cacheData);
//        blockIdToCacheUnit.put(blockId, cacheUnit);
//    }

    private static ArrayList<byte[]> getTuplesWithinTimestamp(ArrayList<byte[]> tuples, Long timestampLowerBound, Long timestampUpperBound)
            throws IOException {

        ArrayList<byte[]> tuplesWithinTimestamp = new ArrayList<>();
        for (int i = 0; i < tuples.size(); ++i) {
            DataTuple dataTuple = schema.deserializeToDataTuple(tuples.get(i));
            Long timestamp = (Long) schema.getValue("timestamp", dataTuple);
            if (timestampLowerBound <= timestamp && timestampUpperBound >= timestamp) {
                tuplesWithinTimestamp.add(tuples.get(i));
            }
        }

        return tuplesWithinTimestamp;
    }

    private BTreeLeafNode getLeafFromExternalStorage(String fileName, int offset)
            throws IOException {

        FileSystemHandler fileSystemHandler = null;
        if (config.HDFSFlag ) {
            fileSystemHandler = new HdfsFileSystemHandler(config.dataDir, config);
        } else {
            fileSystemHandler = new LocalFileSystemHandler(config.dataDir, config);
        }

        byte[] bytesToRead = new byte[4];
        fileSystemHandler.openFile("/", fileName);
//        fileSystemHandler.seek(offset);
        fileSystemHandler.readBytesFromFile(offset, bytesToRead);


        Input input = new Input(bytesToRead);
        int leaveNodeLength = input.readInt();

        bytesToRead = new byte[leaveNodeLength];
        fileSystemHandler.readBytesFromFile(offset + 4, bytesToRead);

//        fileSystemHandler.seek(offset + 4);

        input = new Input(bytesToRead);
//        BTreeLeafNode leaf = kryo.readObject(input, BTreeLeafNode.class);


        fileSystemHandler.closeFile();

        return null;
    }


    private BTreeLeafNode getLeafFromExternalStorage(FileSystemHandler fileSystemHandler, int offset)
            throws IOException {

        Kryo kryo = new Kryo();
        kryo.register(BTree.class, new KryoTemplateSerializer(config));
        kryo.register(BTreeLeafNode.class, new KryoLeafNodeSerializer(config));

//        FileSystemHandler fileSystemHandler = null;
//        if (TopologyConfig.HDFSFlag) {
//            fileSystemHandler = new HdfsFileSystemHandler(TopologyConfig.dataDir);
//        } else {
//            fileSystemHandler = new LocalFileSystemHandler(TopologyConfig.dataDir);
//        }

        byte[] bytesToRead = new byte[4];
//        fileSystemHandler.seek(offset);
        fileSystemHandler.readBytesFromFile(offset, bytesToRead);


        Input input = new Input(bytesToRead);
        int leaveNodeLength = input.readInt();

        bytesToRead = new byte[leaveNodeLength];
//        fileSystemHandler.seek(offset + 4);
        fileSystemHandler.readBytesFromFile(offset + 4, bytesToRead);


        input = new Input(bytesToRead);
        BTreeLeafNode leaf = kryo.readObject(input, BTreeLeafNode.class);


//        fileSystemHandler.closeFile();

        return leaf;
    }


    private static BTreeLeafNode getLeafFromExternalStorage(FileSystemHandler fileSystemHandler, Input input)
            throws IOException {

        Kryo kryo = new Kryo();
        kryo.register(BTree.class, new KryoTemplateSerializer(config));
        kryo.register(BTreeLeafNode.class, new KryoLeafNodeSerializer(config));
//        FileSystemHandler fileSystemHandler = null;
//        if (TopologyConfig.HDFSFlag) {
//            fileSystemHandler = new HdfsFileSystemHandler(TopologyConfig.dataDir);
//        } else {
//            fileSystemHandler = new LocalFileSystemHandler(TopologyConfig.dataDir);
//        }

//        byte[] bytesToRead = new byte[4];
//        fileSystemHandler.seek(offset);
//        fileSystemHandler.readBytesFromFile(offset, bytesToRead);


//        Input input = new Input(bytesToRead);
//        int leaveNodeLength = input.readInt();

//        bytesToRead = new byte[leaveNodeLength];
//        fileSystemHandler.seek(offset + 4);
//        fileSystemHandler.readBytesFromFile(offset + 4, bytesToRead);


//        input = new Input(bytesToRead);
        input.setPosition(input.position() + 4);
        BTreeLeafNode leaf = kryo.readObject(input, BTreeLeafNode.class);


//        fileSystemHandler.closeFile();

        return leaf;
    }


    private Pair getTemplateData(String fileName) {
        Pair data = null;
        try {
            FileSystemHandler fileSystemHandler = null;
            if (config.HDFSFlag) {
                fileSystemHandler = new HdfsFileSystemHandler(config.dataDir, config);
            } else {
                fileSystemHandler = new LocalFileSystemHandler(config.dataDir, config);
            }

            BlockId blockId = new BlockId(fileName, 0);

//            data = (Pair) getFromCache(blockId);
            if (data == null) {
                data = getTemplateFromExternalStorage(fileSystemHandler, fileName);
                CacheData cacheData = new TemplateCacheData(data);
//                putCacheData(blockId, cacheData);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return data;
    }

    private static Pair getTemplateData(FileSystemHandler fileSystemHandler, String fileName) {
        Pair data = null;
        try {
//            FileSystemHandler fileSystemHandler = null;
//            if (TopologyConfig.HDFSFlag) {
//                fileSystemHandler = new HdfsFileSystemHandler(TopologyConfig.dataDir);
//            } else {
//                fileSystemHandler = new LocalFileSystemHandler(TopologyConfig.dataDir);
//            }

            BlockId blockId = new BlockId(fileName, 0);

//            data = (Pair) getFromCache(blockId);
            if (data == null) {
                data = getTemplateFromExternalStorage(fileSystemHandler, fileName);
                CacheData cacheData = new TemplateCacheData(data);
//                putCacheData(blockId, cacheData);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return data;
    }


    private static byte[] readLeafBytesFromFile(FileSystemHandler fileSystemHandler, List<Integer> offsets, int length) {
        Integer endOffset = offsets.get(offsets.size() - 1);
        Integer startOffset = offsets.get(0);

        byte[] bytesToRead = new byte[4];
        fileSystemHandler.readBytesFromFile(endOffset + length + 4, bytesToRead);

        Input input = new Input(bytesToRead);
        int lastLeafLength = input.readInt();

        int totalLength = lastLeafLength + (endOffset - offsets.get(0)) + 4;

        bytesToRead = new byte[totalLength];
        fileSystemHandler.readBytesFromFile(startOffset + length + 4, bytesToRead);

        return bytesToRead;
    }

    private static Pair getTemplateFromExternalStorage(FileSystemHandler fileSystemHandler, String fileName) throws IOException {

        Kryo kryo = new Kryo();
        kryo.register(BTree.class, new KryoTemplateSerializer(config));
        kryo.register(BTreeLeafNode.class, new KryoLeafNodeSerializer(config));
//        fileSystemHandler.openFile("/", fileName);
        byte[] bytesToRead = new byte[4];
        fileSystemHandler.readBytesFromFile(0, bytesToRead);

        Input input = new Input(bytesToRead);
        int templateLength = input.readInt();



        bytesToRead = new byte[templateLength];
        fileSystemHandler.seek(4);
        fileSystemHandler.readBytesFromFile(4, bytesToRead);


        input = new Input(bytesToRead);
        BTree template = kryo.readObject(input, BTree.class);


//        fileSystemHandler.closeFile();

        return new Pair(template, templateLength);
    }
}
