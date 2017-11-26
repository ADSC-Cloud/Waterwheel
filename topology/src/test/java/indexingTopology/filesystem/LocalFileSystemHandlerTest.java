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
        localFileTest.createNewFile("","Test.md");
        localFileTest.openFile("","Test.md");
        // Memchunk : memory chunk
        MemChunk memChunk1 = MemChunk.createNew(str.length());
//        MemChunk memChunk2 = MemChunk.createNew(str2.length());
        Output output = new Output(TopologyConfig.DataChunkHeaderSectionSize);
        output.writeString(str);
        byte[] bytes = output.toBytes();
//        output.clear();
//        output.writeString(str2);
//        byte[] bytes2 = output.toBytes();
        output.close();
//        int flag = str1.length();
        memChunk1.write(bytes);
//        memChunk2.write(bytes2);
        localFileTest.writeToFileSystem(memChunk1,"","test.md");
//        localFileTest.writeToFileSystem(memChunk2,"","test.md");


        byte array1[] = new byte[str.length()+1];
//        byte array2[] = new byte[str2.length()];
        //memChunk.getAllocatedSize();
        localFileTest.readBytesFromFile(array1);
//        localFileTest.readBytesFromFile(6,array2);
//        int b = memChunk.write(array);
        Input input = new Input(array1);
        String result = input.readString();
        assertEquals("Hello World!",result);

        localFileTest.readBytesFromFile(6,array1);
        input = new Input(array1);
        result = input.readString();
        assertEquals("World!",result);
    }

//    @After
//    public void tearDown() {
//        try {
//            Runtime.getRuntime().exec("rm ./Test.md");
//        } catch (IOException e) {
//            e.printStackTrace();
//        }
//    }

}