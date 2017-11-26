package indexingTopology.filesystem;

import clojure.lang.IFn;
import indexingTopology.config.TopologyConfig;
import junit.framework.TestCase;
import org.junit.Test;

import java.io.IOException;

import static org.junit.Assert.*;

public class LocalWritingAndReadHandlerTest extends TestCase{

    public void testLocalWriteAndRead() throws IOException {
        String path = "./";
        boolean append = false;
        LocalWritingHandler writeTest = new LocalWritingHandler(path,append);
        writeTest.openFile("Test.md");
        //byte[] a = new byte[10];
        //byte array[]="Yes".;
        DataChunkHeader header = new DataChunkHeader();
        header.compressionAlgorithm = "lz4";
        header.decompressedDataSize = 10;
        byte[] serialized = header.serialize();
        writeTest.writeToFileSystem(serialized,"Test.md");
        writeTest.closeFile();


        LocalReadingHandler readerTest = new LocalReadingHandler("./");
        readerTest.openFile("Test.md");
        DataChunkHeader deserializedHeader = new DataChunkHeader();
        readerTest.read(serialized);
        assertEquals(TopologyConfig.DataChunkHeaderSectionSize,readerTest.getFileLength());

        deserializedHeader.deserialize(serialized);
        assertEquals("lz4",deserializedHeader.compressionAlgorithm);
        assertEquals(10,deserializedHeader.decompressedDataSize);
    }

    public void tearDown() {
        try {
            Runtime.getRuntime().exec("rm ./Test.md");
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}