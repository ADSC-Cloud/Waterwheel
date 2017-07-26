package indexingTopology.compression;

import net.jpountz.lz4.LZ4SafeDecompressor;
import sun.reflect.generics.reflectiveObjects.NotImplementedException;

/**
 * Created by robert on 26/7/17.
 */
public class Lz4Decompressor implements Decompressor {

    LZ4SafeDecompressor decompressor;

    Lz4Decompressor(LZ4SafeDecompressor decompressor) {
        this.decompressor = decompressor;
    }

    @Override
    public byte[] decompress(byte[] compressed) {
        throw new NotImplementedException();
    }
}
