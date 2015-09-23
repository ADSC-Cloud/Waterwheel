package indexingTopology.util;

import org.junit.Before;
import java.util.List;

import static org.junit.Assert.*;

/**
 * Created by parijatmazumdar on 23/09/15.
 */
public class BTreeTest {
    BTree<Integer,Integer> btree;
    int range;

    private void customInsert(BTree<Integer,Integer> b, int range, int mod,int modResidue) {
        for (int i=0;i<range;i++) {
            if (i%mod==modResidue)
                b.insert(i,100*i);
        }
    }

    private void customDelete(BTree<Integer,Integer> b, int range, int mod,int modResidue) {
        for (int i=0;i<range;i++) {
            if (i%mod==modResidue)
                b.delete(i);
        }
    }

    @Before
    public void setUp() throws Exception {
        btree=new BTree<Integer, Integer>(4);
        range=50;
        customInsert(btree,range,5,0);
        customInsert(btree,range,5,1);
        customInsert(btree,range,5,2);
        customInsert(btree,range,5,3);
        customInsert(btree,range,5,4);
    }

    @org.junit.Test
    public void testSearch() throws Exception {
        for (int i=0;i<range;i++) assertEquals((long) btree.search(i),(long) i * 100);
        assertEquals(btree.search(range+1),null);
    }

    @org.junit.Test
    public void testSearchRange() throws Exception {
        List<Integer> r1 = btree.searchRange(-2,range+2);
        assertEquals((long) r1.size(),range);
        for (int i=0;i<range;i++) assertEquals((long) r1.get(i),(long) i*100);

        for (int start=0;start<range;start++) {
            List<Integer> r2 = btree.searchRange(start,range);
            for (int i=start;i<range;i++) assertEquals((long) r1.get(i-start),(long) i*100);
        }
    }

    @org.junit.Test
    public void testDelete() throws Exception {
        customDelete(btree,range,4,1);

    }
}