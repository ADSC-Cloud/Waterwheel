package indexingTopology.compression;

import org.xerial.snappy.Snappy;

import java.io.IOException;

/**
 * Created by robert on 26/7/17.
 */
public class SnappyCompressor implements Compressor {

    @Override
    public byte[] compress(byte[] decompressed) throws IOException {
        return Snappy.compress(decompressed);
    }
}
