package indexingTopology.filesystem;

import indexingTopology.common.MemChunk;
import indexingTopology.config.TopologyConfig;
import junit.framework.TestCase;

import java.io.IOException;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import org.junit.After;
import org.junit.Test;

import static org.junit.Assert.*;

public class LocalFileSystemHandlerTest {
    @Test
    public void testLocalFileSystem() throws IOException {
        String path = "./";
        String str = "Hello World!";
        TopologyConfig config = new TopologyConfig();
        LocalFileSystemHandler localFileTest = new LocalFileSystemHandler(path, config);
        localFileTest.createNewFile("","asd.txt");
        localFileTest.openFile("","asd.txt");
        // Memchunk : memory chunk
        MemChunk memChunk1 = MemChunk.createNew(str.length());
        Output output = new Output(TopologyConfig.DataChunkHeaderSectionSize);
        output.writeString(str);
        byte[] bytes = output.toBytes();
        output.close();
        memChunk1.write(bytes);
        localFileTest.writeToFileSystem(memChunk1,"","asd.txt");


        byte array1[] = new byte[100];
        localFileTest.readBytesFromFile(array1);
        Input input = new Input(array1);
        String result = input.readString();
        assertEquals("Hello World!",result);

        localFileTest.readBytesFromFile(6,array1);
        input = new Input(array1);
        result = input.readString();
        assertEquals("World!",result);
    }

    @After
    public void tearDown() {
        try {
            Runtime.getRuntime().exec("rm ./asd.txt");
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

}