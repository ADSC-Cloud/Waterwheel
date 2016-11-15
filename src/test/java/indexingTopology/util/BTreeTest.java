package indexingTopology.util;

import indexingTopology.Config.Config;
import indexingTopology.DataSchema;
import indexingTopology.exception.UnsupportedGenericException;
import indexingTopology.util.BTree;
import org.junit.Before;
import org.junit.Test;

import java.io.*;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.junit.Assert.*;

/**
 * Created by parijatmazumdar on 23/09/15.
 */
public class BTreeTest {
/*    BTree<Integer> btree;
    int range;

    private void customInsert(BTree<Integer> b, int range, int mod,int modResidue) {
        for (int i=0;i<range;i++) {
            if (i%mod==modResidue) {
                try {
                    b.insert(i, String.valueOf(100 * i).getBytes());
                } catch (UnsupportedGenericException e) {
                    e.printStackTrace();
                }
            }
        }
    }

//    private void customDelete(BTree<Integer> b, int range, int mod,int modResidue) {
//        for (int i=0;i<range;i++) {
//            if (i%mod==modResidue)
//                b.delete(i);
//        }
//    }

    @Before
    public void setUp() throws Exception {
        btree=new BTree<Integer>(4);
        range=20;
        customInsert(btree, range, 5, 0);
        customInsert(btree, range, 5, 1);
        customInsert(btree, range, 5, 2);
        customInsert(btree, range, 5, 3);
        customInsert(btree, range, 5, 4);
        customInsert(btree, range, 5, 0);
        customInsert(btree, range, 5, 1);
        customInsert(btree, range, 5, 2);
        customInsert(btree, range, 5, 3);
        customInsert(btree, range, 5, 4);
    }

    @org.junit.Test
    public void testSearch() throws Exception {
        for (int i=0;i<range;i++) {
            assertEquals(2,btree.search(i).size());
            assertArrayEquals(String.valueOf(100 * i).getBytes(), btree.search(i).get(0));
        }
    }

    @org.junit.Test
    public void testClear() throws Exception {
        btree.printBtree();
        System.out.println("*******");
        btree.clearPayload();
        btree.printBtree();
        System.out.println("*******");
        try {
            btree.insert(17,String.valueOf(100 * 17).getBytes());
            btree.insert(18,String.valueOf(100 * 18).getBytes());
            btree.insert(19,String.valueOf(100 * 19).getBytes());
            btree.insert(20,String.valueOf(100 * 20).getBytes());
            boolean ret=btree.insert(21,String.valueOf(100 * 21).getBytes());
            assert !ret : "ret not false";
        } catch (UnsupportedGenericException e) {
            e.printStackTrace();
        }
        btree.printBtree();
        System.out.println("*******");
    }

//    @org.junit.Test
//    public void testSearchRange() throws Exception {
//        List<byte[]> r1 = btree.searchRange(-2,range+2);
//        assertEquals((long) r1.size(), range);
//        for (int i=0;i<range;i++) assertArrayEquals(r1.get(i),String.valueOf(100*i).getBytes());
//
//        for (int start=0;start<range;start++) {
//            List<byte[]> r2 = btree.searchRange(start,range);
//            for (int i=start;i<range;i++) assertArrayEquals(String.valueOf(100*i).getBytes(),r2.get(i-start));
//        }
//    }
/*

@org.junit.Test
public void testDelete() throws Exception {
customDelete(btree,range,4,1);
}
*/
       /*
       @Test
       public void testGetTupleFromAChunk() throws IOException {
           SplitCounterModule sm = SplitCounterModule.createNew();
           TimingModule tm = TimingModule.createNew();
           int bTreeOder = 4;
           BTree bTree = new BTree(bTreeOder, tm, sm);
           File inputFile = new File("src/input_data_new");
           BufferedReader bufferedReader = new BufferedReader(new FileReader(inputFile));
           String text = null;
           BytesCounter counter = new BytesCounter();
           BTreeLeafNode leaf = new BTreeLeafNode(4, counter);
           List<Double> values = null;
           Double indexValue = 0.0;
           List<String> fieldNames = new ArrayList<String>(Arrays.asList("user_id", "id_1", "id_2", "ts_epoch",
                   "date", "time", "latitude", "longitude"));
           ArrayList valueTypes = new ArrayList<Class>(Arrays.asList(Double.class, Double.class, Double.class,
                   Double.class, Double.class, Double.class, Double.class, Double.class));
           DataSchema schema = new DataSchema(fieldNames, valueTypes);
           MemChunk chunk = MemChunk.createNew(640000);
//        LinkedBlockingQueue<Pair> queue = new LinkedBlockingQueue<Pair>();
           for (int i = 0; i < 100; ++i) {
               try {
                   text = bufferedReader.readLine();
                   String[] tokens = text.split(" ");
                   values = getValuesObject(tokens);
                   indexValue = values.get(0);
               } catch (IOException e) {
                   e.printStackTrace();
               }
               byte[] serializedTuple = serializeIndexValue(values);
               try {
                   bTree.insert(indexValue, serializedTuple);
               } catch (UnsupportedGenericException e) {
                   e.printStackTrace();
               }
           }
           bTree.printBtree();
           chunk.changeToLeaveNodesStartPosition();
           bTree.writeLeavesIntoChunk(chunk);
           DeserializationHelper deserializationHelper = new DeserializationHelper();
           int len1 = Integer.SIZE / Byte.SIZE;
           int offset1 = 0;
           byte[] serializedTree = bTree.serializeTree();
           BTree deserializedTree = deserializationHelper.deserializeBTree(serializedTree, bTreeOder, counter);
           deserializedTree.printBtree();
           int len = Integer.SIZE / Byte.SIZE;
           int offset = 0;
           chunk.changeToStartPosition();
           chunk.write(serializedTree);
           chunk.changeToStartPosition();
           ByteBuffer byteBuffer = chunk.getData();
           byte[] dataInByte = new byte[640000];
           byteBuffer.get(dataInByte);
           int lengthOfTreeInByte = ByteBuffer.wrap(dataInByte, offset, len).getInt();
           System.out.println(String.format("Tree in byte has %d bytes: ", lengthOfTreeInByte));
           offset += len;
           serializedTree = ByteBuffer.wrap(dataInByte, offset, lengthOfTreeInByte).array();
           deserializedTree = deserializationHelper.deserializeBTree(serializedTree, bTreeOder, counter);
           deserializedTree.printBtree();
//           System.out.println(deserializedTree.getOffsetOfLeaveNodeShouldContainKey(457.6042636844468));
           chunk.changeToSpecificPosition(deserializedTree.getOffsetOfLeaveNodeShouldContainKey(501.78658781297844));
           ByteBuffer byteBufferOfLeave = byteBuffer.slice();
           byte[] leaveInByte = new byte[64000];
           byteBufferOfLeave.get(leaveInByte);
           BTreeLeafNode deserializedLeave = deserializationHelper.deserializeLeaf(leaveInByte, bTreeOder, counter);
           System.out.println(schema.deserialize((byte[]) deserializedLeave.getTuples(0).get(0)));
           deserializedLeave.print();
       } */

       @Test
       public void testRangeSearchGetTupleFromAChunk() throws IOException {
           SplitCounterModule sm = SplitCounterModule.createNew();
           TimingModule tm = TimingModule.createNew();
           int bTreeOder = 4;
           BTree bTree = new BTree(bTreeOder, tm, sm);
           File inputFile = new File("src/input_data_new");
           BufferedReader bufferedReader = new BufferedReader(new FileReader(inputFile));
           String text = null;
           BytesCounter counter = new BytesCounter();
           BTreeLeafNode leaf = new BTreeLeafNode(4, counter);
           List<Double> values = null;
           Double indexValue = 0.0;
           List<String> fieldNames = new ArrayList<String>(Arrays.asList("user_id", "id_1", "id_2", "ts_epoch",
                   "date", "time", "latitude", "longitude"));
           ArrayList valueTypes = new ArrayList<Class>(Arrays.asList(Double.class, Double.class, Double.class,
                   Double.class, Double.class, Double.class, Double.class, Double.class));
           DataSchema schema = new DataSchema(fieldNames, valueTypes);
           MemChunk chunk = MemChunk.createNew(640000);
//        LinkedBlockingQueue<Pair> queue = new LinkedBlockingQueue<Pair>();
           List<Double> indexValueList = new ArrayList<Double>();
           for (int i = 0; i < Config.NUMBER_TUPLES_OF_A_CHUNK; ++i) {
               try {
                   text = bufferedReader.readLine();
                   if (text == null) {
                       bufferedReader.close();
                       bufferedReader = new BufferedReader(new FileReader(inputFile));
                       text = bufferedReader.readLine();
                   }
                   String[] tokens = text.split(" ");
                   values = getValuesObject(tokens);
                   indexValue = values.get(0);
                   indexValueList.add(indexValue);
               } catch (IOException e) {
                   e.printStackTrace();
               }
               byte[] serializedTuple = serializeIndexValue(values);
               try {
                   bTree.insert(indexValue, serializedTuple);
               } catch (UnsupportedGenericException e) {
                   e.printStackTrace();
               }
           }
           bTree.printBtree();
           chunk.changeToLeaveNodesStartPosition();
           bTree.writeLeavesIntoChunk(chunk);
           DeserializationHelper deserializationHelper = new DeserializationHelper();
           int len1 = Integer.SIZE / Byte.SIZE;
           int offset1 = 0;
           byte[] serializedTree = bTree.serializeTree();
//           BTree deserializedTree = deserializationHelper.deserializeBTree(serializedTree, bTreeOder, counter);
//           deserializedTree.printBtree();
           int len = Integer.SIZE / Byte.SIZE;
           int offset = 0;
           chunk.changeToStartPosition();
           chunk.write(serializedTree);
//           chunk.changeToStartPosition();
           /*
           ByteBuffer byteBuffer = chunk.getData();
           byte[] dataInByte = new byte[640000];
           byteBuffer.get(dataInByte);
           int lengthOfTreeInByte = ByteBuffer.wrap(dataInByte, offset, len).getInt();
           System.out.println(String.format("Tree in byte has %d bytes: ", lengthOfTreeInByte));
           offset += len;
           serializedTree = ByteBuffer.wrap(dataInByte, offset, lengthOfTreeInByte).array();
           deserializedTree = deserializationHelper.deserializeBTree(serializedTree, bTreeOder, counter);
           deserializedTree.printBtree();
//           System.out.println(deserializedTree.getOffsetOfLeaveNodeShouldContainKey(457.6042636844468));
           Double leftKey = 448.68542379255587;
           Double rightKey = 542.6783409687173;
           int startPosition = deserializedTree.getOffsetOfLeaveNodeShouldContainKey(leftKey);
           System.out.println("start postion " + startPosition);
           int nextPosition = deserializedTree.getOffsetOfLeaveNodeShouldContainKey(542.6783409687173);
           System.out.println("next position " + nextPosition);
           System.out.println("The offset is " + (nextPosition - startPosition));
           chunk.changeToSpecificPosition(deserializedTree.getOffsetOfLeaveNodeShouldContainKey(leftKey));
//           chunk.changeToSpecificPosition(Config.TEMPLATE_SIZE);
           int lengthOfLeaveInBytes = ByteBuffer.wrap(dataInByte, deserializedTree.getOffsetOfLeaveNodeShouldContainKey(leftKey), len).getInt();
//           System.out.println(lengthOfLeaveInBytes);
           chunk.changeToSpecificPosition(deserializedTree.getOffsetOfLeaveNodeShouldContainKey(leftKey) + len);
           ByteBuffer byteBufferOfLeave = byteBuffer.slice();
           byte[] leaveInByte = new byte[64000];
           byteBufferOfLeave.get(leaveInByte);
           BTreeLeafNode deserializedLeave = deserializationHelper.deserializeLeaf(leaveInByte, bTreeOder, counter);
           System.out.println(schema.deserialize((byte[]) deserializedLeave.getTuples(0).get(0)));
           deserializedLeave.print();



//           lengthOfLeaveInBytes = ByteBuffer.wrap(dataInByte, deserializedTree.getOffsetOfLeaveNodeShouldContainKey(leftKey) + lengthOfLeaveInBytes, len).getInt();
           System.out.println(lengthOfLeaveInBytes);
//           chunk.changeToSpecificPosition(deserializedTree.getOffsetOfLeaveNodeShouldContainKey(leftKey) + len + 1 * Config.LEAVE_NODE_IN_BYTES);
//           chunk.changeToSpecificPosition(deserializedTree.getOffsetOfLeaveNodeShouldContainKey(leftKey) +  1 * Config.LEAVE_NODE_IN_BYTES);
//           System.out.println(deserializedTree.getOffsetOfLeaveNodeShouldContainKey(leftKey) + lengthOfLeaveInBytes + len);
           chunk.changeToSpecificPosition(deserializedTree.getOffsetOfLeaveNodeShouldContainKey(leftKey) + lengthOfLeaveInBytes + len + len);
           int nextLengthOfLeaveInBytes = ByteBuffer.wrap(dataInByte, deserializedTree.getOffsetOfLeaveNodeShouldContainKey(leftKey) + lengthOfLeaveInBytes, len).getInt();
           byteBufferOfLeave = byteBuffer.slice();
           leaveInByte = new byte[64000];
           byteBufferOfLeave.get(leaveInByte);
           deserializedLeave = deserializationHelper.deserializeLeaf(leaveInByte, bTreeOder, counter);
//           System.out.println("Deserialized leave: ");
           deserializedLeave.print();
           System.out.println(schema.deserialize((byte[]) deserializedLeave.getTuples(0).get(0)));
//           deserializedLeave.print();*/
           Collections.sort(indexValueList);

//           for (Double leftKey : indexValueList) {
           Double leftKey = 542.6783409687173;
               System.out.println("Key " + leftKey);
               FileSystemHandler fileSystemHandler = new LocalFileSystemHandler("src");
               fileSystemHandler.writeToFileSystem(chunk, "/", "chunk_test");
               RandomAccessFile file = new RandomAccessFile("src/chunk_test", "r");
               serializedTree = new byte[Config.TEMPLATE_SIZE];
               file.read(serializedTree, 0, Config.TEMPLATE_SIZE);
               BTree deserializedTree = deserializationHelper.deserializeBTree(serializedTree, bTreeOder, counter);
               deserializedTree.printBtree();
               int startPosition = deserializedTree.getOffsetOfLeaveNodeShouldContainKey(leftKey);
               System.out.println("offset " + startPosition);
               byte[] lengthInByte = new byte[4];
               file.seek(startPosition);
               file.read(lengthInByte);
               int lengthOfLeaveInBytes = ByteBuffer.wrap(lengthInByte, 0, 4).getInt();
               System.out.println("Length of leave in bytes " + lengthOfLeaveInBytes);
               byte[] leafInByte = new byte[lengthOfLeaveInBytes];
               file.seek(startPosition + 4);
               file.read(leafInByte);
               BTreeLeafNode deserializedLeaf = deserializationHelper.deserializeLeaf(leafInByte, bTreeOder, counter);
               System.out.println(schema.deserialize((byte[]) deserializedLeaf.getTuples(0).get(0)));
//           file.close();
/*
           startPosition = (startPosition + lengthOfLeaveInBytes + 4);
           lengthInByte = new byte[4];
               file.seek(startPosition);
               file.read(lengthInByte);
           lengthOfLeaveInBytes = ByteBuffer.wrap(lengthInByte, 0, 4).getInt();
               file.seek(startPosition + 4);
               System.out.println("Length of leave in bytes " + lengthOfLeaveInBytes);
               leafInByte = new byte[lengthOfLeaveInBytes];
               file.read(leafInByte);
               deserializedLeaf = deserializationHelper.deserializeLeaf(leafInByte, bTreeOder, counter);
               System.out.println(schema.deserialize((byte[]) deserializedLeaf.getTuples(0).get(0)));*/
//           }
       }

    public List<Double> getValuesObject(String [] valuesAsString) throws IOException {

        List<Double> values = new ArrayList<Double>();
        ArrayList valueTypes = new ArrayList<Class>(Arrays.asList(Double.class, Double.class, Double.class,
                Double.class, Double.class, Double.class, Double.class, Double.class));
        for (int i=0;i < valueTypes.size();i++) {
            if (valueTypes.get(i).equals(Double.class)) {
                values.add(Double.parseDouble(valuesAsString[i]));
            }
        }
        return values;
    }

    public byte[] serializeIndexValue(List<Double> values) throws IOException{
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        ArrayList valueTypes = new ArrayList<Class>(Arrays.asList(Double.class, Double.class, Double.class,
                Double.class, Double.class, Double.class, Double.class, Double.class));
        for (int i = 0;i < valueTypes.size(); ++i) {
            if (valueTypes.get(i).equals(Double.class)) {
                byte [] b = ByteBuffer.allocate(Double.SIZE / Byte.SIZE).putDouble((Double) values.get(i)).array();
                bos.write(b);
            }
        }
        return bos.toByteArray();
    }
}