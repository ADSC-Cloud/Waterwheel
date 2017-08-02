package indexingTopology.compression;

import junit.framework.TestCase;

import java.io.IOException;
import java.util.Arrays;
import java.util.Random;

/**
 * Created by robert on 26/7/17.
 */
public class CompressionTest extends TestCase {
    public void testSnappy() throws IOException {
        byte[] bytes = new byte[10000];
        new Random().nextBytes(bytes);

        Compressor compressor = CompressorFactory.compressor(CompressorFactory.Algorithm.Snappy);
        byte[] compressed = compressor.compress(bytes);

        Decompressor decompressor = CompressorFactory.decompressor(CompressorFactory.Algorithm.Snappy);

        byte[] decompressed = decompressor.decompress(compressed);

        assertTrue(Arrays.equals(bytes, decompressed));
    }
}
