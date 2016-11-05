package indexingTopology.util;

import java.io.*;

/**
 * Created by parijatmazumdar on 09/10/15.
 */
public class BytesCounter implements Serializable{
    /**
     * 4 bytes for storing number of keys
     * 1 byte for storing node type - internal/leaf
     */
    public static final int NUM_NODE_BOOKKEEPING_BYTES =5;
    public static final int NUM_VAL_BOOKKEEPING_BYTES=4;
    public static final int SIZE_OF_INTERGER = 4;
    public static final int SIZE_OF_DOUBLE = 8;
    public static final char SIZE_OF_CHAR = 2;
    private int bytesCountOfTemplate;
    private int bytesCountOfLeaves;
    private int height;
    private int numberOfNodes;
    private int numberOfLeaves;
    public BytesCounter() {
        bytesCountOfTemplate = 0;
        height = 0;
        numberOfNodes = 0;
        numberOfLeaves = 0;
        bytesCountOfLeaves = 0;
    }

    // TODO check
    public int getBytesEstimateForInsert(int keyLen,int valLen) {
        return bytesCountOfTemplate +(height+1)*NUM_NODE_BOOKKEEPING_BYTES+2*keyLen+valLen+NUM_VAL_BOOKKEEPING_BYTES;
    }

    public int getBytesEstimateForLeaveNodes() {
        return 0;
    }

    public int getBytesEstimateForInsertInTemplate() {
//        return bytesCount+keyLen+valLen+NUM_VAL_BOOKKEEPING_BYTES;
        return bytesCountOfTemplate + numberOfNodes * (SIZE_OF_INTERGER + SIZE_OF_CHAR) + numberOfLeaves * SIZE_OF_INTERGER;
    }

    public void countNewNode() {
//        bytesCount += NUM_NODE_BOOKKEEPING_BYTES;
        ++numberOfNodes;
    }

    public void countNumberOfLeaves(int keyCount) {
        numberOfLeaves += keyCount;
    }

    public void increaseHeightCount() {
        height+=1;
    }

    public void countKeyAdditionOfTemplate(int keyLen) {
        bytesCountOfTemplate +=keyLen;
    }

    public void countValueAddition(int valLen) {
        bytesCountOfTemplate +=valLen+NUM_VAL_BOOKKEEPING_BYTES;
    }

    public int getBytesCount() {
        return bytesCountOfTemplate;
    }

    public void countKeyRemovalOfTemplate(int keyLen) {
        bytesCountOfTemplate -=keyLen;
    }

    public void countValueRemoval(int valLen) {
        bytesCountOfTemplate -=valLen+NUM_VAL_BOOKKEEPING_BYTES;
    }

    public int getHeightCount() {
        return height;
    }

    public Object clone() throws CloneNotSupportedException {
        BytesCounter newCounter = new BytesCounter();
        newCounter.bytesCountOfTemplate = bytesCountOfTemplate;
        newCounter.height = height;
        return newCounter;
    }

    public static Object deepClone(Object object) {
        try {
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            ObjectOutputStream oos = new ObjectOutputStream(baos);
            oos.writeObject(object);
            ByteArrayInputStream bais = new ByteArrayInputStream(baos.toByteArray());
            ObjectInputStream ois = new ObjectInputStream(bais);
            return ois.readObject();
        }
        catch (Exception e) {
            e.printStackTrace();
            return null;
        }
    }

    public void setHeight(int height) {
        this.height = height;
    }

    public int getNumberOfNodes() {
        return numberOfNodes;
    }

    public int getNumberOfLeaves() {
        return numberOfLeaves;
    }
}
