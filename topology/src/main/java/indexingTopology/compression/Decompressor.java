package indexingTopology.compression;

import java.io.IOException;

/**
 * Created by robert on 26/7/17.
 */
interface Decompressor {
    byte[] decompress(byte[] compressed) throws IOException;
}
