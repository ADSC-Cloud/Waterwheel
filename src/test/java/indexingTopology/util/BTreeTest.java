package indexingTopology.util;

import indexingTopology.exception.UnsupportedGenericException;
import indexingTopology.util.BTree;
import org.junit.Before;
import java.util.List;

import static org.junit.Assert.*;

/**
 * Created by parijatmazumdar on 23/09/15.
 */
public class BTreeTest {
    BTree<Integer> btree;
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
        range=50;
        customInsert(btree,range,5,0);
        customInsert(btree,range,5,1);
        customInsert(btree,range,5,2);
        customInsert(btree,range,5,3);
        customInsert(btree,range,5,4);
    }

    @org.junit.Test
    public void testSearch() throws Exception {
        for (int i=0;i<range;i++) {
            assertArrayEquals(String.valueOf(100 * i).getBytes(), btree.search(i));
        }

        assertArrayEquals(btree.search(range + 1), null);
    }

    @org.junit.Test
    public void testSearchRange() throws Exception {
        List<byte[]> r1 = btree.searchRange(-2,range+2);
        assertEquals((long) r1.size(), range);
        for (int i=0;i<range;i++) assertArrayEquals(r1.get(i),String.valueOf(100*i).getBytes());

        for (int start=0;start<range;start++) {
            List<byte[]> r2 = btree.searchRange(start,range);
            for (int i=start;i<range;i++) assertArrayEquals(String.valueOf(100*i).getBytes(),r2.get(i-start));
        }
    }
/*

@org.junit.Test
public void testDelete() throws Exception {
customDelete(btree,range,4,1);
}
*/
}