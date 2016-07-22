import indexingTopology.exception.UnsupportedGenericException;
import indexingTopology.util.BTree;
import indexingTopology.util.SplitCounterModule;
import indexingTopology.util.TimingModule;
import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.Random;

/**
 * Created by parijatmazumdar on 28/12/15.
 */
public class WoTemplateMain {
    private static final int btreeOrder = 4;
    private static final int maxTuples = 40000;
    private static BTree<Double,Integer> indexedDataWoTemplate;
    private static int numWrittenWoTemplate;

    public static void main(String [] args) {
        try {
            Random rn = new Random(1000);
            TimingModule tm = TimingModule.createNew();
            SplitCounterModule sm = SplitCounterModule.createNew();
            long startTime = System.nanoTime();
            indexedDataWoTemplate = new BTree<Double,Integer>(btreeOrder, tm, sm);
            int numTuples=0;
            numWrittenWoTemplate=0;
            while (true) {
                tm.reset();
                if (numTuples % maxTuples==0 && numTuples!=0) {
                    long curr = System.nanoTime();
                    System.out.println(curr-startTime);
                    indexedDataWoTemplate = new BTree<Double,Integer>(btreeOrder, tm, sm);
                    numWrittenWoTemplate++;
                    startTime = System.nanoTime();
                }

                numTuples++;
                indexedDataWoTemplate.insert(rn.nextDouble() * 10.0d, numTuples);
            }
        } catch (UnsupportedGenericException e) {
            e.printStackTrace();
        }
    }

}
