package indexingTopology.filesystem;

import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import indexingTopology.config.TopologyConfig;

/**
 * This class contains all the metadata information associated with a data chunk.
 * The instance of this class is supposed to be stored in the header section of the data chunks.
 * When a data chunk is read from the file system, this instance can be reconstructed so that all the metadata
 * information of the data chunk can be easily accessed.
 */
public class DataChunkHeader {
    String compressionAlgorithm;
    int decompressedDataSize;
    String compressionAlgorithm2;
    int decompressedDataSize2;

    byte[] serialize() {
        Output output = new Output(TopologyConfig.DataChunkHeaderSectionSize);

        output.writeString(compressionAlgorithm);
        output.writeInt(decompressedDataSize);
        output.writeString(compressionAlgorithm2);
        output.writeInt(decompressedDataSize2);

        byte[] bytes = output.toBytes();
        output.close();
        byte[] headSection = new byte[TopologyConfig.DataChunkHeaderSectionSize];
        System.arraycopy(bytes, 0, headSection, 0, bytes.length);
        return headSection;
    }

    void deserialize(byte[] bytes) {
        if (bytes.length < TopologyConfig.DataChunkHeaderSectionSize)
            throw new RuntimeException("bytes should be large than " + TopologyConfig.DataChunkHeaderSectionSize + " " +
                    "as configured in DataCHunkHeaderSectionSize in TopologyConfig");

        Input input = new Input(bytes);
        compressionAlgorithm = input.readString();
        decompressedDataSize = input.readInt();
        compressionAlgorithm2 = input.readString();
        decompressedDataSize2 = input.readInt();
    }
}
