package indexingTopology.util;

import indexingTopology.Config.Config;

import java.nio.ByteBuffer;

/**
 * Created by parijatmazumdar on 18/01/16.
 */
public class MemChunk {
    private int allocatedSize;
    private ByteBuffer data;
    private MemChunk() {
        allocatedSize = 0;
        data = null;
    }

    private MemChunk(int allocateSize) {
        assert allocateSize>=5 : "allocate size should be greater than 5";
        this.allocatedSize = allocateSize;
        data = ByteBuffer.allocateDirect(allocateSize);
    }

    public static MemChunk createNew(int allocateSize) {
        return new MemChunk(allocateSize);
    }

    public int write(byte [] serialData) {
        int offset = data.position();
        if (allocatedSize - offset < serialData.length)
            return -1;

        data.put(serialData,0,serialData.length);
        return offset;
    }

    public final byte [] serializeAndRefresh() {
        int offset = data.position();
        byte [] ret = new byte[offset];
        data.position(0);
        data.get(ret);
        data.position(0);
        return ret;
    }

    public ByteBuffer getData() {
        return data;
    }

    public int getOffset() {
        return data.position();
    }

    public void changeToLeaveNodesStartPosition() {
        data.position(Config.TEMPLATE_SIZE);
    }
    public void changeToStartPosition() {
        data.position(0);
    }
}
