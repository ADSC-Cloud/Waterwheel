import indexingTopology.exception.UnsupportedGenericException;
import indexingTopology.util.BTree;
import indexingTopology.util.SplitCounterModule;
import indexingTopology.util.TimingModule;
import org.apache.commons.exec.Executor;
import org.apache.hadoop.hdfs.server.namenode.SafeMode;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;


public class WithTemplateMain {
    private static final int btreeOrder = 4;
    private static final int maxTuples = 40000;
    private static BTree<Double,Integer> indexedData;
    private static ExecutorService es;
    private static final int numThreads = 3;

    private static class IndexerThread implements Runnable {
        private final BTree<Double,Integer> index;
        private final double indexValue;
        private final int offset;

        public IndexerThread(BTree<Double,Integer> index,double indexValue,int offset) {
            this.index = index;
            this.offset = offset;
            this.indexValue = indexValue;
        }

        public void run() {
            try {
                index.insert(indexValue,offset);
            } catch (UnsupportedGenericException e) {
                e.printStackTrace();
            }
        }
    }

    private static void shutdownAndRestartThreadPool() {
        es.shutdown();
        try {
            es.awaitTermination(60, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        es = Executors.newFixedThreadPool(numThreads);
    }

    public static void main(String [] args) {
        Random rn = new Random(1000);
        TimingModule tm = TimingModule.createNew();
        SplitCounterModule sm = SplitCounterModule.createNew();
        es = Executors.newFixedThreadPool(1);
        indexedData=new BTree<Double,Integer>(btreeOrder,tm,sm);
        long startTime = System.nanoTime();
        int numTuples=0;
        while (true) {
            tm.reset();
            if (numTuples % maxTuples==0 && numTuples!=0) {
                shutdownAndRestartThreadPool();
                long curr = System.nanoTime();
                System.out.println(curr - startTime);
                indexedData.clearPayload();
                startTime = System.nanoTime();
            }

            numTuples++;
            es.submit(new IndexerThread(indexedData,rn.nextDouble() * 10.0d, numTuples));
        }
    }
}
