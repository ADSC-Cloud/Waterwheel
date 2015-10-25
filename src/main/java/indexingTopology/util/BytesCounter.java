package indexingTopology.util;

/**
 * Created by parijatmazumdar on 09/10/15.
 */
public class BytesCounter {
    /**
     * 4 bytes for storing number of keys
     * 1 byte for storing node type - internal/leaf
     */
    public static final int NUM_NODE_BOOKKEEPING_BYTES =5;
    public static final int NUM_VAL_BOOKKEEPING_BYTES=4;
    private int count;
    private int height;
    public BytesCounter() {
        count=0;
        height=0;
    }

    // TODO check
    public int getBytesEstimateForInsert(int keyLen,int valLen) {
        return count+(height+1)*NUM_NODE_BOOKKEEPING_BYTES+2*keyLen+valLen+NUM_VAL_BOOKKEEPING_BYTES;
    }

    public void countNewNode() {
        count+= NUM_NODE_BOOKKEEPING_BYTES;
    }

    public void increaseHeightCount() {
        height+=1;
    }

    public void countKeyAddition(int keyLen) {
        count+=keyLen;
    }

    public void countValueAddition(int valLen) {
        count+=valLen+NUM_VAL_BOOKKEEPING_BYTES;
    }

    public int getCount() {
        return count;
    }

    public void countKeyRemoval(int keyLen) {
        count-=keyLen;
    }

    public void countValueRemoval(int valLen) {
        count-=valLen+NUM_VAL_BOOKKEEPING_BYTES;
    }

    public int getHeightCount() {
        return height;
    }
}
