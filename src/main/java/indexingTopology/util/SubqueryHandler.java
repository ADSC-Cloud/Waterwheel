package indexingTopology.util;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import indexingTopology.aggregator.Aggregator;
import indexingTopology.cache.LRUCache;
import indexingTopology.config.TopologyConfig;
import indexingTopology.data.DataSchema;
import indexingTopology.data.DataTuple;
import indexingTopology.filesystem.FileSystemHandler;
import indexingTopology.filesystem.HdfsFileSystemHandler;
import indexingTopology.filesystem.LocalFileSystemHandler;
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
    public List<byte[]> handleSubquery(SubQueryOnFile subQuery) throws IOException {
        Long queryId = subQuery.getQueryId();
        TKey leftKey =  (TKey) subQuery.getLeftKey();
        TKey rightKey =  (TKey) subQuery.getRightKey();
        String fileName = subQuery.getFileName();
        Long timestampLowerBound = subQuery.getStartTimestamp();
        Long timestampUpperBound = subQuery.getEndTimestamp();


        FileSystemHandler fileSystemHandler = null;
        if (config.HDFSFlag) {
            if (config.HybridStorage && new File(config.dataDir, fileName).exists()) {
                fileSystemHandler = new LocalFileSystemHandler(config.dataDir, config);
                System.out.println("Subquery will be conducted on local file in cache.");
            } else {
                System.out.println("Failed to find local file :" + config.dataDir + "/" + fileName);
                fileSystemHandler = new HdfsFileSystemHandler(config.dataDir, config);
            }
        } else {
            fileSystemHandler = new LocalFileSystemHandler(config.dataDir, config);
        }

        fileSystemHandler.openFile("/", fileName);

        byte[] chunkBytes = getChunkBytes(fileSystemHandler, fileName);

        Pair data = getTemplateData(chunkBytes);

        BTree template = (BTree) data.getKey();
        Integer length = (Integer) data.getValue();


        BTreeLeafNode leaf = null;
        ArrayList<byte[]> tuples = new ArrayList<byte[]>();


        List<Integer> offsets = template.getOffsetsOfLeafNodesShouldContainKeys(leftKey, rightKey);


        //code below are used to evaluate the specific time cost
        Long keyRangeTime = 0L;
        Long timestampRangeTime = 0L;
        Long predicationTime = 0L;
        Long aggregationTime = 0L;


        for (Integer offset : offsets) {
            Input input = new Input(chunkBytes);
            input.setPosition(offset + length + 4);
            leaf = kryo.readObject(input, BTreeLeafNode.class);

            Long startTime = System.currentTimeMillis();


            ArrayList<byte[]> tuplesInKeyRange = leaf.getTuplesWithinKeyRange(leftKey, rightKey);

            keyRangeTime += System.currentTimeMillis() - startTime;

            //deserialize
            List<DataTuple> dataTuples = new ArrayList<>();
            tuplesInKeyRange.stream().forEach(e -> dataTuples.add(schema.deserializeToDataTuple(e)));

            //filter by timestamp range

            startTime = System.currentTimeMillis();

            filterByTimestamp(dataTuples, timestampLowerBound, timestampUpperBound);


            timestampRangeTime += System.currentTimeMillis() - startTime;


            //filter by predicate
//            System.out.println("Before predicates: " + dataTuples.size());
            startTime = System.currentTimeMillis();

            filterByPredicate(dataTuples, subQuery.getPredicate());

            predicationTime += System.currentTimeMillis() - startTime;
//            System.out.println("After predicates: " + dataTuples.size());


            startTime = System.currentTimeMillis();

            if (subQuery.getAggregator() != null) {
                Aggregator.IntermediateResult intermediateResult = subQuery.getAggregator().createIntermediateResult();
                subQuery.getAggregator().aggregate(dataTuples, intermediateResult);
                dataTuples.clear();
                dataTuples.addAll(subQuery.getAggregator().getResults(intermediateResult).dataTuples);
            }

            aggregationTime += System.currentTimeMillis() - startTime;


            //serialize
            List<byte[]> serializedDataTuples = new ArrayList<>();

            if (subQuery.getAggregator() != null) {
                DataSchema outputSchema = subQuery.getAggregator().getOutputDataSchema();
                dataTuples.stream().forEach(p -> serializedDataTuples.add(outputSchema.serializeTuple(p)));
            } else {
                dataTuples.stream().forEach(p -> serializedDataTuples.add(schema.serializeTuple(p)));
            }


            tuples.addAll(serializedDataTuples);
        }

        return tuples;
    }

    private void filterByTimestamp(List<DataTuple> tuples, Long timestampLowerBound, Long timestampUpperBound)
            throws IOException {

        List<DataTuple> result =
                tuples.stream().filter(p -> {
                    Long timestamp = (Long) schema.getValue("timestamp", p);
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
