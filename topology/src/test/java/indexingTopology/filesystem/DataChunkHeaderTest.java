package indexingTopology.filesystem;

import indexingTopology.config.TopologyConfig;

import junit.framework.TestCase;
/**
 * Created by robert on 26/7/17.
 */
public class DataChunkHeaderTest extends TestCase{

    public void testSerialization() {
        DataChunkHeader header = new DataChunkHeader();
        header.compressionAlgorithm = "lz4";
        header.decompressedDataSize = 10000;
        byte[] serialized = header.serialize();
        assertEquals(TopologyConfig.DataChunkHeaderSectionSize, serialized.length);

        DataChunkHeader deserializedHeader = new DataChunkHeader();
        deserializedHeader.deserialize(serialized);

        assertEquals("lz4", deserializedHeader.compressionAlgorithm);
        assertEquals(10000, deserializedHeader.decompressedDataSize);

    }
}