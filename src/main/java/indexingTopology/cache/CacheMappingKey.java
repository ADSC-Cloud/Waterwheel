package indexingTopology.cache;

/**
 * Created by acelzj on 11/29/16.
 */
public class CacheMappingKey {

    private String fileName;

    private int offset;

    public CacheMappingKey(String fileName, int offset) {
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
        if (!(obj instanceof CacheMappingKey)) {
            return false;
        }
        if (obj == this) {
            return true;
        }

        CacheMappingKey thatCacheMappingKey = (CacheMappingKey) obj;
        return thatCacheMappingKey.fileName.equals(fileName) && thatCacheMappingKey.offset == offset;
    }

}
