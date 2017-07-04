package indexingTopology.util;

import indexingTopology.bolt.ChunkScanner;
import indexingTopology.config.TopologyConfig;
import indexingTopology.common.data.DataSchema;
import indexingTopology.common.data.DataTuple;
import indexingTopology.exception.UnsupportedGenericException;
import indexingTopology.filesystem.FileSystemHandler;
import indexingTopology.filesystem.HdfsFileSystemHandler;
import indexingTopology.filesystem.LocalFileSystemHandler;
import junit.framework.TestCase;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * Created by acelzj on 5/25/17.
 */
public class SubqueryHandlerTest extends TestCase {

    private TopologyConfig config = new TopologyConfig();
    List<Class> valueTypes = new ArrayList<Class>(Arrays.asList(Double.class, Double.class, String.class));


    public void setUp() {
        try {
            Runtime.getRuntime().exec("mkdir -p ./target/tmp");
        } catch (IOException e) {
            e.printStackTrace();
        }
        config.dataDir = "./target/tmp";
        config.HDFSFlag = false;
        config.CHUNK_SIZE = 1024 * 1024;
        System.out.println("dataDir is set to " + config.dataDir);
    }

    public void tearDown() {
        try {
            Runtime.getRuntime().exec("rm ./target/tmp/*");
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void testHandleSubquery() throws Exception, UnsupportedGenericException {

        config.ChunkOrientedCaching = true;

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
        ChunkScanner.DebugInfo info = new ChunkScanner.DebugInfo();
        List<byte[]> tuples = subqueryHandler.handleSubquery(subQueryOnFile, info);

        assertEquals(100000, tuples.size());
    }

    @Test
    public void testHandleSubqueryOnOneLayerTemplate() throws InterruptedException, IOException {
        config.CHUNK_SIZE = 58000000 / 4;
        config.ChunkOrientedCaching = true;

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
        ChunkScanner.DebugInfo info = new ChunkScanner.DebugInfo();
        List<byte[]> tuples = subqueryHandler.handleSubquery(subQueryOnFile, info);

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