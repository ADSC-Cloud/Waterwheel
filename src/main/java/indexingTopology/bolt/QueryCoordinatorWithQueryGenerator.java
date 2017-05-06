package indexingTopology.bolt;

import indexingTopology.data.PartialQueryResult;
import indexingTopology.util.*;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

/**
 * Created by acelzj on 11/15/16.
 */
public class QueryCoordinatorWithQueryGenerator<T extends Number & Comparable<T>> extends QueryCoordinator<T> {


    private long queryId;

    private Thread queryGenerationThread;

    private int sleepTimeInSeconds = 300;

    private static final Logger LOG = LoggerFactory.getLogger(QueryCoordinatorWithQueryGenerator.class);

    public QueryCoordinatorWithQueryGenerator(T lowerBound, T upperBound) {
        super(lowerBound, upperBound);
    }

    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        super.prepare(map, topologyContext, outputCollector);
        queryId = 0;
//        queryGenerationThread = new Thread(new QueryRunnable());
//        queryGenerationThread.start();

    }

    @Override
    public void handlePartialQueryResult(Long queryId, PartialQueryResult partialQueryResult) {
        // nothing to do.
    }

    class QueryRunnable implements Runnable {

        public void run() {
            Long startTimestamp = System.currentTimeMillis();
            try {
                Thread.sleep(sleepTimeInSeconds * 1000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            while (queryId < 30) {
                try {
                    Thread.sleep(10 * 1000);

//                    network 0.01
//                    Integer leftKey = 236120000;
//                    Integer rightKey = 236800000;

//                    network 0.05
//                    Integer leftKey = 1074640000;
//                    Integer rightKey = 1709410000;

//                    network 0.1
                    Integer leftKey = 1001380000;
                    Integer rightKey = 1780920000;


//                    network 0.25
//                    Integer leftKey = 1001380000;
//                    Integer rightKey = 1944520000;

//                    network 0.5
//                    Integer leftKey = -755870000;
//                    Integer rightKey = 1944520000;

//                    network 0.75
//                    Integer leftKey = -2147340000;
//                    Integer rightKey = 1786280000;

//                    network 1
//                    Integer leftKey = -2147341772;
//                    Integer rightKey = 2118413319;


//                    taxi 0.01
//                    Integer leftKey = 3000;
//                    Integer rightKey = 17000;

//                    taxi 0.05
//                    Integer leftKey = 3000;
//                    Integer rightKey = 52000;

//                    taxi 0.1
//                    Integer leftKey = 3000;
//                    Integer rightKey = 108000;

//                    taxi 0.25
//                    Integer leftKey = 3000;
//                    Integer rightKey = 151000;

//                    taxi 0.5
//                    Integer leftKey = 3000;
//                    Integer rightKey = 198000;

//                    taxi 0.75
//                    Integer leftKey = 3000;
//                    Integer rightKey = 233000;


//                    taxi 1
//                    Integer leftKey = 0;
//                    Integer rightKey = 1048576;

//                    Integer leftKey = 3000;
//                    Integer rightKey = 108000;

//                    double selectivity = 0.1;

//                    Long endTimestamp =  ((int) ((end - startTimestamp) * Math.sqrt(selectivity))) + startTimestamp;
//                    Long endTimestamp = System.currentTimeMillis();
//                    Long startTimestamp = endTimestamp - sleepTimeInSeconds * 1000;
                    startTimestamp += 10 * 1000;
                    Long endTimestamp = startTimestamp + sleepTimeInSeconds * 1000;

//                    Long startTimestamp = 0L;
//                    Long endTimestamp = Long.MAX_VALUE;

                    final List<Query<T>> queryList = new ArrayList<>();
                    queryList.add(new Query(queryId, leftKey, rightKey, startTimestamp, endTimestamp));
                    pendingQueue.put(queryList);
                } catch (Exception e) {
                    e.printStackTrace();
                }

                ++queryId;

            }
        }
    }

}
