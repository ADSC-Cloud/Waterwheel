package indexingTopology.util;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import indexingTopology.bolt.QueryServerBolt;
import indexingTopology.common.SubQueryOnFile;
import indexingTopology.common.aggregator.Aggregator;
import indexingTopology.cache.LRUCache;
import indexingTopology.config.TopologyConfig;
import indexingTopology.common.data.DataSchema;
import indexingTopology.common.data.DataTuple;
import indexingTopology.filesystem.*;
import indexingTopology.index.BTree;
import indexingTopology.index.BTreeLeafNode;
import indexingTopology.index.KryoLeafNodeSerializer;
import indexingTopology.index.KryoTemplateSerializer;
import javafx.util.Pair;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Predicate;
import java.util.stream.Collectors;

/**
 * Created by acelzj on 5/25/17.
 */
public class SubqueryHandler<TKey extends Number & Comparable<TKey>> {
    private transient LRUCache<String, byte[]> chunkNameToChunkMapping;

    private transient Kryo kryo;

    private DataSchema schema;

    TopologyConfig config;

    public SubqueryHandler(DataSchema schema, TopologyConfig config) {
        this.schema = schema;
        this.config = config;

        chunkNameToChunkMapping = new LRUCache<>(10);
        kryo = new Kryo();
        kryo.register(BTree.class, new KryoTemplateSerializer(config));
        kryo.register(BTreeLeafNode.class, new KryoLeafNodeSerializer(config));
    }

    @SuppressWarnings("unchecked")
    public List<byte[]> handleSubquery(SubQueryOnFile subQuery, QueryServerBolt.DebugInfo debugInfo) throws IOException {
        ArrayList<byte[]> tuples = new ArrayList<byte[]>();
        Long queryId = subQuery.getQueryId();
        TKey leftKey =  (TKey) subQuery.getLeftKey();
        TKey rightKey =  (TKey) subQuery.getRightKey();
        String fileName = subQuery.getFileName();
        FileSystemHandler fileSystemHandler = null;


        ReadingHandler readingHandler = null;


        debugInfo.runningPosition = "breakpoint 1";

        try {
            Long timestampLowerBound = subQuery.getStartTimestamp();
            Long timestampUpperBound = subQuery.getEndTimestamp();


//            if (config.HDFSFlag) {
//                if (config.HybridStorage && new File(config.dataChunkDir, fileName).exists()) {
//                    fileSystemHandler = new LocalFileSystemHandler(config.dataChunkDir, config);
//                    System.out.println("Subquery will be conducted on local file in cache.");
//                } else {
//                    if (config.HybridStorage)
//                        System.out.println("Failed to find local file :" + config.dataChunkDir + "/" + fileName);
//                    fileSystemHandler = new HdfsFileSystemHandler(config.dataChunkDir, config);
//                }
//            } else {
//                fileSystemHandler = new LocalFileSystemHandler(config.dataChunkDir, config);
//            }



//
            debugInfo.runningPosition = "breakpoint 2";

            if (config.HDFSFlag) {
                if (config.HybridStorage && new File(config.dataChunkDir, fileName).exists()) {
                    readingHandler = new LocalReadingHandler(config.dataChunkDir);
                    System.out.println("Subquery will be conducted on local file in cache.");
                } else {
                    if (config.HybridStorage)
                        System.out.println("Failed to find local file :" + config.dataChunkDir + "/" + fileName);
                    readingHandler = new HdfsReadingHandler(config.dataChunkDir, config);
                }
            } else {
                readingHandler = new LocalReadingHandler(config.dataChunkDir);
            }



//            fileSystemHandler.openFile("/", fileName);
            readingHandler.openFile(fileName);

//            byte[] chunkBytes = getChunkBytes(fileSystemHandler, fileName);
            debugInfo.runningPosition = "breakpoint 3";
            byte[] chunkBytes = getChunkBytes(readingHandler, fileName);
            debugInfo.runningPosition = "breakpoint 4";

//            debugInfo.runningPosition = "breakpoint 3";
//            fileSystemHandler.openFile("/", fileName);
//            debugInfo.runningPosition = "breakpoint 4";



//            byte[] chunkBytes = getChunkBytes(fileSystemHandler, fileName);
            debugInfo.runningPosition = "breakpoint 5";
            Pair data = getTemplateData(chunkBytes);
            debugInfo.runningPosition = "breakpoint 6";
            BTree template = (BTree) data.getKey();
            Integer length = (Integer) data.getValue();


            BTreeLeafNode leaf = null;

            debugInfo.runningPosition = "breakpoint 7";
            List<Integer> offsets = template.getOffsetsOfLeafNodesShouldContainKeys(leftKey, rightKey);
            debugInfo.runningPosition = "breakpoint 8";

            //code below are used to evaluate the specific time cost
            Long keyRangeTime = 0L;
            Long timestampRangeTime = 0L;
            Long predicationTime = 0L;
            Long aggregationTime = 0L;

            int count = 0;
            debugInfo.runningPosition = "breakpoint 9";
            for (Integer offset : offsets) {
                Input input = new Input(chunkBytes);
                input.setPosition(offset + length + 4);
                leaf = kryo.readObject(input, BTreeLeafNode.class);

                Long startTime = System.currentTimeMillis();
                debugInfo.runningPosition = String.format("breakpoint 9.%d.1", count);

                ArrayList<byte[]> tuplesInKeyRange = leaf.getTuplesWithinKeyRange(leftKey, rightKey);

                keyRangeTime += System.currentTimeMillis() - startTime;
                debugInfo.runningPosition = String.format("breakpoint 9.%d.2", count);
                //deserialize
                List<DataTuple> dataTuples = new ArrayList<>();
                tuplesInKeyRange.stream().forEach(e -> dataTuples.add(schema.deserializeToDataTuple(e)));
                debugInfo.runningPosition = String.format("breakpoint 9.%d.3", count);
                //filter by timestamp range

                startTime = System.currentTimeMillis();

                filterByTimestamp(dataTuples, timestampLowerBound, timestampUpperBound);

                debugInfo.runningPosition = String.format("breakpoint 9.%d.4", count);
                timestampRangeTime += System.currentTimeMillis() - startTime;


                //filter by predicate
            System.out.println("Before predicates: " + dataTuples.size());
                startTime = System.currentTimeMillis();

                filterByPredicate(dataTuples, subQuery.getPredicate());
                debugInfo.runningPosition = String.format("breakpoint 9.%d.5", count);
                predicationTime += System.currentTimeMillis() - startTime;
            System.out.println("After predicates: " + dataTuples.size());


                startTime = System.currentTimeMillis();

                if (subQuery.getAggregator() != null) {
                    Aggregator.IntermediateResult intermediateResult = subQuery.getAggregator().createIntermediateResult();
                    subQuery.getAggregator().aggregate(dataTuples, intermediateResult);
                    dataTuples.clear();
                    dataTuples.addAll(subQuery.getAggregator().getResults(intermediateResult).dataTuples);
                }

                aggregationTime += System.currentTimeMillis() - startTime;
                debugInfo.runningPosition = String.format("breakpoint 9.%d.6", count);

                //serialize
                List<byte[]> serializedDataTuples = new ArrayList<>();

                if (subQuery.getAggregator() != null) {
                    DataSchema outputSchema = subQuery.getAggregator().getOutputDataSchema();
                    dataTuples.stream().forEach(p -> serializedDataTuples.add(outputSchema.serializeTuple(p)));
                } else {
                    dataTuples.stream().forEach(p -> serializedDataTuples.add(schema.serializeTuple(p)));
                }
                debugInfo.runningPosition = String.format("breakpoint 9.%d.7", count);

                tuples.addAll(serializedDataTuples);
            }

        } finally {
//            if (fileSystemHandler != null) {
//                fileSystemHandler.closeFile();
//            }
            if (readingHandler != null) {
                readingHandler.closeFile();
            }
        }
        debugInfo.runningPosition = "breakpoint 10";
        return tuples;
    }

    private void filterByTimestamp(List<DataTuple> tuples, Long timestampLowerBound, Long timestampUpperBound)
            throws IOException {

        List<DataTuple> result =
                tuples.stream().filter(p -> {
//                    Long timestamp = (Long) schema.getValue("timestamp", p);
                    Long timestamp = (Long) schema.getTemporalValue(p);
                    return timestampLowerBound <= timestamp && timestampUpperBound >= timestamp;
                }).collect(Collectors.toList());
        tuples.clear();
        tuples.addAll(result);

    }

    private void filterByPredicate(List<DataTuple> tuples, Predicate<DataTuple> predicate) {
        if (predicate != null) {
            List<DataTuple> result = tuples.stream().filter(predicate).collect(Collectors.toList());
            tuples.clear();
            tuples.addAll(result);
        }
    }

    private byte[] getFromCache(String chunkName) {
        if (chunkNameToChunkMapping.get(chunkName) == null) {
            return null;
        }
        return chunkNameToChunkMapping.get(chunkName);
    }

    private byte[] getFromExternalStorage(FileSystemHandler fileSystemHandler) {
        byte[] bytesToRead = new byte[4];
        fileSystemHandler.readBytesFromFile(0, bytesToRead);

        Input input = new Input(bytesToRead);
        int chunkLength = input.readInt();

        bytesToRead = new byte[chunkLength];
        fileSystemHandler.readBytesFromFile(4, bytesToRead);
        return bytesToRead;
    }


    private byte[] getChunkBytes(FileSystemHandler fileSystemHandler, String chunkName) {
        byte[] chunkBytes;

        chunkBytes = getFromCache(chunkName);
        if (chunkBytes == null) {
            chunkBytes = getFromExternalStorage(fileSystemHandler);
            chunkNameToChunkMapping.put(chunkName, chunkBytes);
        }

        return chunkBytes;
    }

    private byte[] getChunkBytes(ReadingHandler readingHandlerHandler, String chunkName) throws IOException {
        byte[] chunkBytes;

        chunkBytes = getFromCache(chunkName);
        if (chunkBytes == null) {
            chunkBytes = getFromExternalStorage(readingHandlerHandler);
            chunkNameToChunkMapping.put(chunkName, chunkBytes);
        }

        return chunkBytes;
    }

    private byte[] getFromExternalStorage(ReadingHandler readingHandler) throws IOException {
        long chunkLength = readingHandler.getFileLength();

        byte[] bytesToRead = new byte[(int) chunkLength];
        readingHandler.read(bytesToRead);

        return bytesToRead;
    }

    private Pair getTemplateData(byte[] chunkBytes) {
        Pair data = null;

        //get the template length
        Input input = new Input(chunkBytes);
        int templateLength = input.readInt();

        //deserialize the bytes to in-memory template
        BTree template = kryo.readObject(input, BTree.class);

        return new Pair(template, templateLength);
    }
}
