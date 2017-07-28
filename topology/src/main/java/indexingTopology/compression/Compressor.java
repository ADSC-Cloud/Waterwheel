package indexingTopology.compression;

import java.io.IOException;

/**
 * Created by robert on 26/7/17.
 */
public interface Compressor {
    byte[] compress(byte[] decompressed) throws IOException;
}
