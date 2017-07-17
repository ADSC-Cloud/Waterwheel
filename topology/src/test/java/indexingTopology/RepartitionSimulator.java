package indexingTopology;

import indexingTopology.config.TopologyConfig;
import indexingTopology.util.BalancedPartition;
import indexingTopology.util.RepartitionManager;
import org.apache.commons.math3.distribution.NormalDistribution;
import org.apache.storm.utils.Utils;

import java.util.Random;
import java.util.concurrent.Semaphore;

/**
 * Created by acelzj on 3/3/17.
 */
public class RepartitionSimulator {

    static int sleepTimeInSeconds = 30;
    static int repatitionWatingTimeInSeconds = 30;
    static NormalDistribution distribution;
    static Random random;
    static double offset;
    static double mean;
    static double sigma;
    static BalancedPartition balancedPartition;
    static Semaphore repartitionSemaphore;
    static int numberOfPartitions = 8;
    static int lowerBound;
    static int upperBound;

    public static void main(String[] args) {
        sigma = 100000;
        mean = 500000;
        lowerBound = (int) (mean - 3*sigma);
        upperBound = (int) (mean + 3 * sigma);
        balancedPartition = new BalancedPartition<>(numberOfPartitions, 100, lowerBound, upperBound, true);
        repartitionSemaphore = new Semaphore(1);
        offset = 80000;
        random = new Random();
        distribution = new NormalDistribution(mean, sigma);
        createDistributionChangingThread();
        createDataGeneratingThread();
        createRepartitionThread();
    }

    private static void createDistributionChangingThread() {
        new Thread(new Runnable() {
            @Override
            public void run() {
                while (true) {
                    Utils.sleep(sleepTimeInSeconds * 1000);
                    Double randomValue = random.nextDouble();
                    if (randomValue < 0.5) {
                        distribution = new NormalDistribution(mean + offset, sigma);
                    } else {
                        distribution = new NormalDistribution(mean - offset, sigma);
                    }
                }
            }
        }).start();
    }

    private static void createDataGeneratingThread() {
        new Thread(new Runnable() {
            @Override
            public void run() {
                while (true) {
                    try {
                        repartitionSemaphore.acquire();
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }

                    Integer value = (int) distribution.sample();
//                    System.out.println(balancedPartition.getIntervalId(value % upperBound));
                    balancedPartition.record(value % upperBound);

                    repartitionSemaphore.release();
                }
            }
        }).start();
    }


    private static void createRepartitionThread() {
        new Thread(new Runnable() {
            @Override
            public void run() {
                while (true) {
                    Utils.sleep(repatitionWatingTimeInSeconds * 1000);
                    try {
                        repartitionSemaphore.acquire();
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }

                    System.out.println(balancedPartition.getIntervalDistribution().getHistogram());

                    RepartitionManager repartitionManager = new RepartitionManager(numberOfPartitions, 1024, balancedPartition.getIntervalToPartitionMapping(), balancedPartition.getIntervalDistribution());

                    System.out.println(repartitionManager.getRepartitionPlan());

                    balancedPartition.setIntervalToPartitionMapping(repartitionManager.getRepartitionPlan());

                    repartitionSemaphore.release();
                }
            }
        }).start();
    }
}
