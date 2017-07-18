package indexingTopology.bloom;

import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.google.common.hash.BloomFilter;
import indexingTopology.config.TopologyConfig;
import indexingTopology.filesystem.FileSystemHandler;
import indexingTopology.filesystem.LocalFileSystemHandler;
import indexingTopology.common.MemChunk;
import org.apache.commons.lang.SerializationUtils;
import org.apache.storm.shade.org.eclipse.jetty.util.ConcurrentHashSet;
import org.apache.storm.shade.org.jboss.netty.util.internal.ConcurrentHashMap;

import java.io.IOException;
import java.util.Set;
import java.util.Map;

/**
 * This class maintains bloom filter in file system and provides an access interface.
 */
public class BloomFilterStore {

    Map<BloomFilterId, BloomFilter> idToInMemoryFilter = new ConcurrentHashMap<>();
    Set<BloomFilterId> registeredBloomFilters = new ConcurrentHashSet<>();
    TopologyConfig config;

    public BloomFilterStore(TopologyConfig config) {
        this.config = config;
    }

    static public class BloomFilterId {
        String column;
        String chunkName;

        public BloomFilterId(String chunkName, String column) {
            this.column = column;
            this.chunkName = chunkName;
        }
        @Override
        public String toString() {
            return chunkName + "-" + column;
        }

        public boolean equals(Object o) {
            return o instanceof BloomFilterId &&
                    ((BloomFilterId) o).chunkName.equals(this.chunkName) &&
                    ((BloomFilterId) o).column.equals(this.column);
        }

        public int hashCode() {
            return (column + chunkName).hashCode();
        }
    }

    public BloomFilter get(BloomFilterId id) {
        if (!registeredBloomFilters.contains(id))
            return null;
        FileSystemHandler fileSystemHandler;
        fileSystemHandler = new LocalFileSystemHandler(config.dataDir, config);
        fileSystemHandler.openFile("/", id.toString());

        byte[] lengthBytes = new byte[4];
        fileSystemHandler.readBytesFromFile(lengthBytes);

        Input input = new Input(lengthBytes);
        int length = input.readInt();
        byte[] contentBytes = new byte[length];
        fileSystemHandler.readBytesFromFile(4, contentBytes);
        fileSystemHandler.closeFile();
        BloomFilter filter = (BloomFilter) SerializationUtils.deserialize(contentBytes);
        return filter;
    }

    public void store(BloomFilterId id, BloomFilter filter) throws IOException {
        byte[] bytes = SerializationUtils.serialize(filter);
        MemChunk memChunk = MemChunk.createNew(bytes.length + 4);

        Output output = new Output(4);
        output.writeInt(bytes.length);
        memChunk.write(output.toBytes());
        output.close();

        memChunk.write(bytes);
        FileSystemHandler fileSystemHandler;
        fileSystemHandler = new LocalFileSystemHandler(config.dataDir, config);
        fileSystemHandler.writeToFileSystem(memChunk, "/", id.toString());
        registeredBloomFilters.add(id);
    }

}
