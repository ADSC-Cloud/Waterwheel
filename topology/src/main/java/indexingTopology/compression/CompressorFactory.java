package indexingTopology.compression;

import net.jpountz.lz4.LZ4Factory;

/**
 * Created by robert on 26/7/17.
 */
public class CompressorFactory {

    enum Algorithm {LZ4, Snappy}
    static public Compressor compressor(Algorithm algorithm) {
        switch (algorithm) {
            case LZ4:
                return new Lz4Compressor(LZ4Factory.fastestInstance().fastCompressor());
            case Snappy:
                return new SnappyCompressor();
            default:
                return null;
        }
    }

    static public Decompressor decompressor(Algorithm algorithm) {
        switch (algorithm) {
            case LZ4:
                return new Lz4Decompressor(LZ4Factory.fastestInstance().safeDecompressor());
            case Snappy:
                return new SnappyDecompressor();
            default:
                return null;
        }
    }

}
