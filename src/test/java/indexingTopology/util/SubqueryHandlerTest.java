package indexingTopology.util;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Output;
import com.sun.tools.doclets.internal.toolkit.util.IndexBuilder;
import indexingTopology.config.TopologyConfig;
import indexingTopology.data.DataSchema;
import indexingTopology.data.DataTuple;
import indexingTopology.exception.UnsupportedGenericException;
import indexingTopology.filesystem.FileSystemHandler;
import indexingTopology.filesystem.HdfsFileSystemHandler;
import indexingTopology.filesystem.LocalFileSystemHandler;
import indexingTopology.util.taxi.Car;
import indexingTopology.util.taxi.City;
import indexingTopology.util.taxi.TrajectoryGenerator;
import indexingTopology.util.taxi.TrajectoryUniformGenerator;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.SynchronousQueue;

import static org.junit.Assert.*;

/**
 * Created by acelzj on 5/25/17.
 */
public class SubqueryHandlerTest {

    private TopologyConfig config = new TopologyConfig();
    List<Class> valueTypes = new ArrayList<Class>(Arrays.asList(Double.class, Double.class, String.class));

    @Test
    public void TestHandleSubquery() throws Exception, UnsupportedGenericException {

        config.CHUNK_SIZE = 58000000 / 4;

        int numTuples = 120000;

        DataSchema schema = new DataSchema();
        schema.addIntField("a1");
        schema.addLongField("timestamp");
        schema.addVarcharField("a4", 100);
        schema.setPrimaryIndexField("a1");

        IndexerBuilder indexerBuilder = new IndexerBuilder(config);

        LinkedBlockingQueue inputQueue = new LinkedBlockingQueue();

        Indexer indexer = indexerBuilder.setTaskId(0)
                .setDataSchema(schema)
                .setInputQueue(inputQueue)
                .setQueryPendingQueue(new ArrayBlockingQueue<SubQuery>(1024))
                .getIndexer();
//        for (int j = 0; j < 10; ++j) {
        for (int i = 0; i < numTuples; ++i) {
            DataTuple tuple = new DataTuple();
            tuple.add(i);
            tuple.add(System.currentTimeMillis());
            tuple.add("payload");
            inputQueue.put(tuple);
        }

        indexer.writeTreeIntoChunk();

        MemChunk chunk = indexer.getChunk();

        String chunkName = "taskId0Chunk0";

        FileSystemHandler fileSystemHandler = null;
        if (config.HDFSFlag) {
            fileSystemHandler = new HdfsFileSystemHandler(config.dataDir, config);
        } else {
            fileSystemHandler = new LocalFileSystemHandler(config.dataDir, config);
        }
        fileSystemHandler.writeToFileSystem(chunk, "/", chunkName);

        SubQueryOnFile subQueryOnFile = new SubQueryOnFile(0L, 1, 100000, chunkName, 0L, Long.MAX_VALUE, null, null, null);

        SubqueryHandler subqueryHandler = new SubqueryHandler(schema, config);

        List<byte[]> tuples = subqueryHandler.handleSubquery(subQueryOnFile);

        assertEquals(100000, tuples.size());
    }

    @Test
    public void TestHandleSubqueryOnOneLayerTemplate() throws InterruptedException, IOException {
        config.CHUNK_SIZE = 58000000 / 4;

        int numTuples = 60;

        DataSchema schema = new DataSchema();
        schema.addIntField("a1");
        schema.addLongField("timestamp");
        schema.addVarcharField("a4", 100);
        schema.setPrimaryIndexField("a1");

        IndexerBuilder indexerBuilder = new IndexerBuilder(config);

        LinkedBlockingQueue inputQueue = new LinkedBlockingQueue();

        Indexer indexer = indexerBuilder.setTaskId(0)
                .setDataSchema(schema)
                .setInputQueue(inputQueue)
                .setQueryPendingQueue(new ArrayBlockingQueue<SubQuery>(1024))
                .getIndexer();
//        for (int j = 0; j < 10; ++j) {
        for (int i = 0; i < numTuples; ++i) {
            DataTuple tuple = new DataTuple();
            tuple.add(i);
            tuple.add(System.currentTimeMillis());
            tuple.add("payload");
            inputQueue.put(tuple);
        }

        indexer.writeTreeIntoChunk();

        MemChunk chunk = indexer.getChunk();

        String chunkName = "taskId0Chunk0";

        FileSystemHandler fileSystemHandler = null;
        if (config.HDFSFlag) {
            fileSystemHandler = new HdfsFileSystemHandler(config.dataDir, config);
        } else {
            fileSystemHandler = new LocalFileSystemHandler(config.dataDir, config);
        }
        fileSystemHandler.writeToFileSystem(chunk, "/", chunkName);

        SubQueryOnFile subQueryOnFile = new SubQueryOnFile(0L, 0, numTuples, chunkName, 0L, Long.MAX_VALUE, null, null, null);

        SubqueryHandler subqueryHandler = new SubqueryHandler(schema, config);

        List<byte[]> tuples = subqueryHandler.handleSubquery(subQueryOnFile);

        assertEquals(numTuples, tuples.size());
    }

    public byte[] serializeIndexValue(List<Object> values) throws IOException {
        ByteArrayOutputStream bos = new ByteArrayOutputStream();

        for (int i = 0;i < valueTypes.size(); ++i) {
            if (valueTypes.get(i).equals(Double.class)) {
                byte [] b = ByteBuffer.allocate(Double.SIZE / Byte.SIZE).putDouble((Double) values.get(i)).array();
                bos.write(b);
            } else if (valueTypes.get(i).equals(String.class)) {
                byte [] b = ((String) values.get(i)).getBytes();
                byte [] sizeHeader = ByteBuffer.allocate(Integer.SIZE/ Byte.SIZE).putInt(b.length).array();
                bos.write(sizeHeader);
                bos.write(b);
            } else {
                throw new IOException("Only classes supported till now are string and double");
            }
        }

        byte [] b = ByteBuffer.allocate(Long.SIZE / Byte.SIZE).putLong((Long) values.get(valueTypes.size())).array();
        bos.write(b);
        return bos.toByteArray();
    }

}