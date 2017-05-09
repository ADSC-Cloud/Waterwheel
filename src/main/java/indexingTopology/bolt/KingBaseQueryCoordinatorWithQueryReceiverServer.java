package indexingTopology.bolt;

import indexingTopology.aggregator.Aggregator;
import indexingTopology.client.*;
import indexingTopology.data.DataTuple;
import indexingTopology.data.PartialQueryResult;
import indexingTopology.util.DataTuplePredicate;
import indexingTopology.util.Query;
import indexingTopology.util.texi.City;
import indexingTopology.util.texi.Interval;
import indexingTopology.util.texi.Intervals;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.List;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Created by acelzj on 11/15/16.
 */
public class KingBaseQueryCoordinatorWithQueryReceiverServer<T extends Number & Comparable<T>> extends QueryCoordinator<T> {

    private final int port;

    AtomicLong queryId;

    Server server;

    Map<Long, LinkedBlockingQueue<PartialQueryResult>> queryIdToPartialQueryResults;

    City city;

//    Map<Long, Semaphore> queryIdToPartialQueryResultSemphore;

    private static final Logger LOG = LoggerFactory.getLogger(KingBaseQueryCoordinatorWithQueryReceiverServer.class);

    public KingBaseQueryCoordinatorWithQueryReceiverServer(T lowerBound, T upperBound, int port, City city) {
        super(lowerBound, upperBound);
        this.port = port;
        this.city = city;
    }

    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        super.prepare(map, topologyContext, outputCollector);
        queryId = new AtomicLong(0);
        queryIdToPartialQueryResults = new HashMap<>();


        server = new Server(port, QueryServerHandle.class, new Class[]{LinkedBlockingQueue.class, AtomicLong.class, Map.class, City.class}, pendingQueue, queryId, queryIdToPartialQueryResults, city);
        new Thread(new Runnable() {
            @Override
            public void run() {
                try {
                    server.startDaemon();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }).start();
//        queryIdToPartialQueryResultSemphore = new HashMap<>();
    }

    @Override
    public void handlePartialQueryResult(Long queryId, PartialQueryResult partialQueryResult) {
        LinkedBlockingQueue<PartialQueryResult> results = queryIdToPartialQueryResults.computeIfAbsent(queryId, k -> new LinkedBlockingQueue<>());

        try {
            results.put(partialQueryResult);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

//        Semaphore semaphore = queryIdToPartialQueryResultSemphore.computeIfAbsent(queryId, k -> new Semaphore(0));
//        semaphore.release();
    }

    static public class QueryServerHandle<T extends Number & Comparable<T>> extends ServerHandle implements QueryHandle, GeoTemporalQueryHandle {

        LinkedBlockingQueue<List<Query<T>>> pendingQueryQueue;
        AtomicLong queryIdGenerator;
        AtomicLong superQueryIdGenerator;
        Map<Long, LinkedBlockingQueue<PartialQueryResult>> queryresults;
        City city;
        public QueryServerHandle(LinkedBlockingQueue<List<Query<T>>> pendingQueryQueue, AtomicLong queryIdGenerator, Map<Long, LinkedBlockingQueue<PartialQueryResult>> queryresults, City city) {
            this.pendingQueryQueue = pendingQueryQueue;
            this.queryresults = queryresults;
            this.queryIdGenerator = queryIdGenerator;
            this.city = city;
        }

        @Override
        public void handle(QueryRequest request) throws IOException {
            try {
                final long queryid = queryIdGenerator.getAndIncrement();

                LinkedBlockingQueue<PartialQueryResult> results =
                        queryresults.computeIfAbsent(queryid, k -> new LinkedBlockingQueue<>());

                LOG.info("A new Query{} ({}, {}, {}, {}) is added to the pending queue.", queryid,
                        request.low, request.high, request.startTime, request.endTime);
                final List<Query<T>> queryList = new ArrayList<>();
                queryList.add(new Query(queryid, request.low, request.high, request.startTime,
                        request.endTime, request.predicate, request.aggregator));
                pendingQueryQueue.put(queryList);

                System.out.println("Admitted a query.  waiting for query results");
                boolean eof = false;
                while(!eof) {
                    System.out.println("Before take!");
                    PartialQueryResult partialQueryResult = results.take();
                    System.out.println("Received PartialQueryResult!");
                    eof = partialQueryResult.getEOFFlag();
                    objectOutputStream.writeUnshared(new QueryResponse(partialQueryResult, queryid));
                    objectOutputStream.reset();
                }

            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }

        @Override
        public void handle(GeoTemporalQueryRequest clientQueryRequest) throws IOException {
            try {
                final long queryid = queryIdGenerator.getAndIncrement();

                LinkedBlockingQueue<PartialQueryResult> results =
                        queryresults.computeIfAbsent(queryid, k -> new LinkedBlockingQueue<>());

                final List<Query<T>> queryList = new ArrayList<>();
                final long startTimeStamp = clientQueryRequest.startTime;
                final long endTimeStamp = clientQueryRequest.endTime;
                final DataTuplePredicate predicate = clientQueryRequest.predicate;
                final Aggregator aggregator = clientQueryRequest.aggregator;

                Intervals intervals = city.getZCodeIntervalsInARectagle(clientQueryRequest.x1.doubleValue(),
                        clientQueryRequest.x2.doubleValue(),
                        clientQueryRequest.y1.doubleValue(),
                        clientQueryRequest.y2.doubleValue());

                for (Interval interval: intervals.intervals) {
                    queryList.add(new Query(queryid, interval.low, interval.high, startTimeStamp, endTimeStamp,
                            predicate, aggregator));
                    LOG.info("A new Query{} ({}, {}, {}, {}) is added to the pending queue.", queryid,
                            interval.low, interval.high, startTimeStamp, endTimeStamp);
                }

                pendingQueryQueue.put(queryList);

                System.out.println("Admitted a query.  waiting for query results");
                boolean eof = false;
                while(!eof) {
                    System.out.println("Before take!");
                    PartialQueryResult partialQueryResult = results.take();
                    System.out.println("Received PartialQueryResult!");
                    eof = partialQueryResult.getEOFFlag();
                    objectOutputStream.writeUnshared(new QueryResponse(partialQueryResult, queryid));
                    objectOutputStream.reset();
                }

            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }

    }
}
