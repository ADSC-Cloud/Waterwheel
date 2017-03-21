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

    private static final Logger LOG = LoggerFactory.getLogger(QueryCoordinatorWithQueryGenerator.class);

    public QueryCoordinatorWithQueryGenerator(T lowerBound, T upperBound) {
        super(lowerBound, upperBound);
    }

    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        super.prepare(map, topologyContext, outputCollector);
//        queryGenerationThread = new Thread(new QueryRunnable());
//        queryGenerationThread.start();

    }

    @Override
    public void handlePartialQueryResult(Long queryId, PartialQueryResult partialQueryResult) {
        // nothing to do.
    }

    class QueryRunnable implements Runnable {

        public void run() {
            while (true) {
                try {
                    Thread.sleep(1000);

//                    Integer leftKey = 0;
//                    Integer rightKey = 0.25;

//                    Long leftKey = 0L;
//                    Long rightKey = 1000L;

                    Double leftKey = 0.0;
                    Double rightKey = 0.01;

                    Long startTimestamp = (long) 0;
                    Long endTimestamp = Long.MAX_VALUE;

                    pendingQueue.put(new Query(queryId, leftKey, rightKey, startTimestamp, endTimestamp));
                } catch (Exception e) {
                    e.printStackTrace();
                }

                ++queryId;

            }
        }
    }

}
