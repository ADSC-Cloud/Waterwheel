package indexingTopology.util;

/**
 * Created by parijatmazumdar on 09/10/15.
 */
public class BytesCounter implements Cloneable{
    /**
     * 4 bytes for storing number of keys
     * 1 byte for storing node type - internal/leaf
     */
    public static final int NUM_NODE_BOOKKEEPING_BYTES =5;
    public static final int NUM_VAL_BOOKKEEPING_BYTES=4;
    private int bytesCount;
    private int height;
    public BytesCounter() {
        bytesCount =0;
        height=0;
    }

    // TODO check
    public int getBytesEstimateForInsert(int keyLen,int valLen) {
        return bytesCount +(height+1)*NUM_NODE_BOOKKEEPING_BYTES+2*keyLen+valLen+NUM_VAL_BOOKKEEPING_BYTES;
    }

    public int getBytesEstimateForInsertInTemplate(int keyLen,int valLen) {
        return bytesCount+keyLen+valLen+NUM_VAL_BOOKKEEPING_BYTES;
    }

    public void countNewNode() {
        bytesCount += NUM_NODE_BOOKKEEPING_BYTES;
    }

    public void increaseHeightCount() {
        height+=1;
    }

    public void countKeyAddition(int keyLen) {
        bytesCount +=keyLen;
    }

    public void countValueAddition(int valLen) {
        bytesCount +=valLen+NUM_VAL_BOOKKEEPING_BYTES;
    }

    public int getBytesCount() {
        return bytesCount;
    }

    public void countKeyRemoval(int keyLen) {
        bytesCount -=keyLen;
    }

    public void countValueRemoval(int valLen) {
        bytesCount -=valLen+NUM_VAL_BOOKKEEPING_BYTES;
    }

    public int getHeightCount() {
        return height;
    }

    public Object clone() throws CloneNotSupportedException{
        BytesCounter newCounter = null;
        try {
            newCounter = (BytesCounter) super.clone();
        } catch (CloneNotSupportedException e) {
            e.printStackTrace();
        }
        newCounter.bytesCount = bytesCount;
        newCounter.height = height;
        return newCounter;
    }
}
