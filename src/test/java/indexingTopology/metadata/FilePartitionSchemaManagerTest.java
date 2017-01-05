package indexingTopology.metadata;

import org.junit.Test;

import static org.junit.Assert.*;

/**
 * Created by acelzj on 12/22/16.
 */
public class FilePartitionSchemaManagerTest {

    @Test
    public void searchInKeyRangeAndTimestampRange() throws Exception {
        FilePartitionSchemaManager partitionSchemaManager = new FilePartitionSchemaManager();
        partitionSchemaManager.add(new FileMetaData("file 1", 50, 100, 20000, 30000));
        partitionSchemaManager.add(new FileMetaData("file 2", 20, 100, 22000, 28000));
        assertEquals("file 1", partitionSchemaManager.search(60, 61, 20000, 25000).get(0));
        assertEquals("file 2", partitionSchemaManager.search(60, 61, 20000, 25000).get(1));
        assertEquals(2, partitionSchemaManager.search(60, 61, 20000, 25000).size());
    }

    @Test
    public void searchNotInKeyRangeButTimestampRange() throws Exception {
        FilePartitionSchemaManager partitionSchemaManager = new FilePartitionSchemaManager();
        partitionSchemaManager.add(new FileMetaData("file 1", 50, 100, 20000, 30000));
        partitionSchemaManager.add(new FileMetaData("file 2", 20, 100, 22000, 28000));
        assertEquals(0, partitionSchemaManager.search(200, Integer.MAX_VALUE, 20000, 25000).size());
    }

    @Test
    public void searchNotInTimestampRangeButKeyRange() throws Exception {
        FilePartitionSchemaManager partitionSchemaManager = new FilePartitionSchemaManager();
        partitionSchemaManager.add(new FileMetaData("file 1", 50, 100, 20000, 30000));
        partitionSchemaManager.add(new FileMetaData("file 2", 20, 100, 22000, 28000));
        assertEquals(0, partitionSchemaManager.search(0, 100, 0, 10000).size());
    }

    @Test
    public void searchNotInTimestampRangeAndKeyRange() throws Exception {
        FilePartitionSchemaManager partitionSchemaManager = new FilePartitionSchemaManager();
        partitionSchemaManager.add(new FileMetaData("file 1", 50, 100, 20000, 30000));
        partitionSchemaManager.add(new FileMetaData("file 2", 20, 100, 22000, 28000));
        assertEquals(0, partitionSchemaManager.search(110, 120, 31000, 320000).size());
    }

}