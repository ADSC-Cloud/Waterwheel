package indexingTopology.bolt;

import indexingTopology.api.client.GeoTemporalQueryRequest;
import indexingTopology.api.client.QueryRequest;
import indexingTopology.api.client.QueryResponse;
import indexingTopology.api.server.GeoTemporalQueryHandle;
import indexingTopology.api.server.QueryHandle;
import indexingTopology.api.server.Server;
import indexingTopology.api.server.ServerHandle;
import indexingTopology.common.Query;
import indexingTopology.common.aggregator.Aggregator;
import indexingTopology.common.data.DataSchema;
import indexingTopology.common.data.PartialQueryResult;
import indexingTopology.common.logics.DataTupleEquivalentPredicateHint;
import indexingTopology.common.logics.DataTuplePredicate;
import indexingTopology.common.logics.DataTupleSorter;
import indexingTopology.config.TopologyConfig;
import indexingTopology.util.taxi.City;
import indexingTopology.util.taxi.Interval;
import indexingTopology.util.taxi.Intervals;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicLong;

public class GeoTemporalQueryCoordinatorBoltBolt<T extends Number & Comparable<T>> extends QueryCoordinatorBolt<T> {

    private final int port;

    AtomicLong queryId;

    Server server;

    Map<Long, LinkedBlockingQueue<PartialQueryResult>> queryIdToPartialQueryResults;

    City city;

//    Map<Long, Semaphore> queryIdToPartialQueryResultSemphore;

    private static final Logger LOG = LoggerFactory.getLogger(GeoTemporalQueryCoordinatorBoltBolt.class);

    public GeoTemporalQueryCoordinatorBoltBolt(T lowerBound, T upperBound, int port, City city,
                                               TopologyConfig config, DataSchema schema) {
        super(lowerBound, upperBound, config, schema);
        this.port = port;
        this.city = city;
    }

    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        super.prepare(map, topologyContext, outputCollector);
        queryId = new AtomicLong(0);
        queryIdToPartialQueryResults = new HashMap<>();


        server = new Server(port, QueryServerHandle.class, new Class[]{LinkedBlockingQueue.class, AtomicLong.class,
                Map.class, City.class, DataSchema.class},
                pendingQueue, queryId, queryIdToPartialQueryResults, city, schema);
        server.startDaemon();
//        queryIdToPartialQueryResultSemphore = new HashMap<>();
    }

    @Override
    public void cleanup() {
        server.endDaemon();
        super.cleanup();
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
        DataSchema schema;
        public QueryServerHandle(LinkedBlockingQueue<List<Query<T>>> pendingQueryQueue, AtomicLong queryIdGenerator,
                                 Map<Long, LinkedBlockingQueue<PartialQueryResult>> queryresults, City city,
                                 DataSchema schema) {
            this.pendingQueryQueue = pendingQueryQueue;
            this.queryresults = queryresults;
            this.queryIdGenerator = queryIdGenerator;
            this.city = city;
            this.schema = schema;
        }

        @Override
        public void handle(QueryRequest request) throws IOException {
            try {
                final long queryid = queryIdGenerator.getAndIncrement();

                DataSchema outputSchema;
                if (request.aggregator != null) {
                    outputSchema = request.aggregator.getOutputDataSchema();
                } else {
                    outputSchema = schema;
                }

                LinkedBlockingQueue<PartialQueryResult> results =
                        queryresults.computeIfAbsent(queryid, k -> new LinkedBlockingQueue<>());

                LOG.info("A new Query{} ({}, {}, {}, {}) is added to the pending queue.", queryid,
                        request.low, request.high, request.startTime, request.endTime);
                final List<Query<T>> queryList = new ArrayList<>();
                queryList.add(new Query(queryid, request.low, request.high, request.startTime,
                        request.endTime, request.predicate, request.postPredicate, request.aggregator, request.sorter, request.equivalentPredicate));
                pendingQueryQueue.put(queryList);

                System.out.println("Admitted a query.  waiting for query results");
                boolean eof = false;
                while(!eof) {
//                    System.out.println("Before take!");
                    PartialQueryResult partialQueryResult = results.take();
//                    System.out.println("Received PartialQueryResult!");
                    eof = partialQueryResult.getEOFFlag();
                    objectOutputStream.writeUnshared(new QueryResponse(partialQueryResult, outputSchema, queryid));
                    objectOutputStream.reset();
                }

            } catch (InterruptedException e) {
                e.printStackTrace();
                Thread.interrupted();
            }
        }

        @Override
        public void handle(GeoTemporalQueryRequest clientQueryRequest) throws IOException {
            try {

                long prepareTimeStart = System.currentTimeMillis();
                final long queryid = queryIdGenerator.getAndIncrement();

                DataSchema outputSchema;
                if (clientQueryRequest.aggregator != null) {
                    outputSchema = clientQueryRequest.aggregator.getOutputDataSchema();
                } else {
                    outputSchema = schema;
                }

                LinkedBlockingQueue<PartialQueryResult> results =
                        queryresults.computeIfAbsent(queryid, k -> new LinkedBlockingQueue<>());

                final List<Query<T>> queryList = new ArrayList<>();
                final long startTimeStamp = clientQueryRequest.startTime;
                final long endTimeStamp = clientQueryRequest.endTime;
                final DataTuplePredicate predicate = clientQueryRequest.predicate;
                final DataTuplePredicate postPredicate = clientQueryRequest.postPredicate;
                final Aggregator aggregator = clientQueryRequest.aggregator;
                final DataTupleSorter sorter = clientQueryRequest.sorter;
                final DataTupleEquivalentPredicateHint equivalentPredicate = clientQueryRequest.equivalentPredicate;

                Intervals intervals = city.getZCodeIntervalsInARectagle(clientQueryRequest.x1.doubleValue(),
                        clientQueryRequest.x2.doubleValue(),
                        clientQueryRequest.y1.doubleValue(),
                        clientQueryRequest.y2.doubleValue());

                for (Interval interval: intervals.intervals) {
                    queryList.add(new Query(queryid, interval.low, interval.high, startTimeStamp, endTimeStamp,
                            predicate, postPredicate, aggregator, sorter, equivalentPredicate));
                    LOG.info("A new Query{} ({}, {}, {}, {}) is added to the pending queue.", queryid,
                            interval.low, interval.high, startTimeStamp, endTimeStamp);
                }

                pendingQueryQueue.put(queryList);

                System.out.println("Admitted a query.  waiting for query results");
                boolean eof = false;
                while(!eof) {
//                    System.out.println("Before take!");
                    PartialQueryResult partialQueryResult = results.take();
//                    System.out.println("Received PartialQueryResult!");
                    eof = partialQueryResult.getEOFFlag();
                    objectOutputStream.writeUnshared(new QueryResponse(partialQueryResult, outputSchema, queryid));
                    objectOutputStream.reset();
                }

            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }

    }
}
