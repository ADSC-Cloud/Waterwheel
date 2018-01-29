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

        Compressor compressorWithLz4 = CompressorFactory.compressor(CompressorFactory.Algorithm.Snappy);
        byte[] compressedWithLz4 = compressorWithLz4.compress(bytes);

        Decompressor decompressorWithLz4 = CompressorFactory.decompressor(CompressorFactory.Algorithm.Snappy);

        byte[] decompressedWithLz4 = decompressorWithLz4.decompress(compressedWithLz4);

        assertTrue(Arrays.equals(bytes, decompressedWithLz4));
    }
}
