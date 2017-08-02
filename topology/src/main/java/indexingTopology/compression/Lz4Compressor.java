package indexingTopology.compression;

import net.jpountz.lz4.LZ4Compressor;
import net.jpountz.lz4.LZ4Factory;

import java.util.Arrays;

/**
 * Created by robert on 26/7/17.
 */
public class Lz4Compressor implements Compressor {

    LZ4Compressor compressor;

    Lz4Compressor(LZ4Compressor lz4Compressor) {
        this.compressor = lz4Compressor;
    }

    @Override
    public byte[] compress(byte[] data) {
        int maxCompressedLength = compressor.maxCompressedLength(data.length);
        byte[] compressed = new byte[maxCompressedLength];
        int compressedLength = compressor.compress(data, 0, data.length, compressed, 0, maxCompressedLength);
        return Arrays.copyOf(compressed, compressedLength);
    }
}
