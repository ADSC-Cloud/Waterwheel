package indexingTopology.util;

import java.nio.ByteBuffer;

/**
 * Created by parijatmazumdar on 18/01/16.
 */
public class MemChunk {
    private int allocatedSize;
    private ByteBuffer data;
    private static final int INITIAL_OFFSET = 4;
    private MemChunk() {
        allocatedSize = 0;
        data = null;
    }

    private MemChunk(int allocateSize) {
        assert allocateSize>=5 : "allocate size should be greater than 5";
        this.allocatedSize = allocateSize;
        data = ByteBuffer.allocateDirect(allocateSize);
        data.position(INITIAL_OFFSET);
    }

    public static MemChunk createNew(int allocateSize) {
        return new MemChunk(allocateSize);
    }

    public int write(byte [] serialData) {
        int offset = data.position();
        if (allocatedSize - offset < serialData.length)
            return -1;

        data.put(serialData,0,serialData.length);
        assert data.position() == offset+serialData.length : "Sanity check, position should be 4";
        return offset;
    }

    public final byte [] serializeAndRefresh() {
        int offset = data.position();
        data.position(0);
        data.put(ByteBuffer.allocate(INITIAL_OFFSET).putInt(offset));
        assert data.position() == INITIAL_OFFSET : "Sanity check, position should be 4";
        return data.array();
    }

    public int getOffset() {
        return data.position();
    }
}
