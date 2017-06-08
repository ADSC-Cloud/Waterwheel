package indexingTopology.cache;

/**
 * Created by acelzj on 11/29/16.
 */
public class BlockId {

    private String fileName;

    private int offset;

    public BlockId(String fileName, int offset) {
        this.fileName = fileName;
        this.offset = offset;
    }

    @Override
    public int hashCode() {
        int result = 17;
        result = result + fileName.hashCode();
        result = result + offset;
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (!(obj instanceof BlockId)) {
            return false;
        }
        if (obj == this) {
            return true;
        }

        BlockId thatBlockId = (BlockId) obj;
        return thatBlockId.fileName.equals(fileName) && thatBlockId.offset == offset;
    }

}
