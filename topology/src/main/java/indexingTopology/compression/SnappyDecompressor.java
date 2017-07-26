package indexingTopology.compression;

import org.xerial.snappy.Snappy;

import java.io.IOException;

/**
 * Created by robert on 26/7/17.
 */
public class SnappyDecompressor implements Decompressor {

    @Override
    public byte[] decompress(byte[] compressed) throws IOException {
        return Snappy.uncompress(compressed);
    }
}
