package indexingTopology.util;

import com.esotericsoftware.kryo.io.Input;
import indexingTopology.metadata.FileMetaData;
import indexingTopology.metadata.FilePartitionSchemaManager;
import org.junit.Test;

import static org.junit.Assert.*;

/**
 * Created by acelzj on 24/2/17.
 */
public class ZookeeperHandlerTest {
    /*
    @Test
    public void create() throws Exception {
        ZookeeperHandler handler = new ZookeeperHandler();

        String s = "";

        handler.create("/MyFirstZnode", s.getBytes());
    }

    @Test
    public void setData() throws Exception {
        ZookeeperHandler handler = new ZookeeperHandler();

        String s = "HelloWorld";

        handler.setData("/MyFirstZnode", s.getBytes());

        assertEquals(s, new String(handler.getData("/MyFirstZnode"), "UTF-8"));
    }

    @Test
    public void getData() throws Exception {
        byte[] bytes;
        String path = "/MetadataNode";

        ZookeeperHandler handler = new ZookeeperHandler();
        bytes = handler.getData(path);
        Input input = new Input(bytes);

        int numberOfMetadata = input.readInt();

//        System.out.println(numberOfMetadata);

        FilePartitionSchemaManager filePartitionSchemaManager = new FilePartitionSchemaManager();

        for (int i = 0; i < numberOfMetadata; ++i) {
            String fileName = input.readString();
            Double lowerBound = input.readDouble();
            Double upperBound = input.readDouble();
            Long startTimestamp = input.readLong();
            Long endTimestamp = input.readLong();

//            filePartitionSchemaManager.add(new FileMetaData(fileName, lowerBound, upperBound, startTimestamp,endTimestamp));
//            System.out.println(fileName + " " + lowerBound + " " + upperBound + " " + startTimestamp + " " + endTimestamp);

        }

//        System.out.println(filePartitionSchemaManager.search(0, 0.1, 0, Long.MAX_VALUE).size());
//        System.out.println(filePartitionSchemaManager.search(0, 0.01, 0, Long.MAX_VALUE).size());
    }
    */
}