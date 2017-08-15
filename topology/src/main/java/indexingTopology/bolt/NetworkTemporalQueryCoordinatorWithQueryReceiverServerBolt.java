package indexingTopology.bolt;

import indexingTopology.api.client.NetworkTemporalQueryRequest;
import indexingTopology.api.client.QueryRequest;
import indexingTopology.api.client.QueryResponse;
import indexingTopology.api.server.*;
import indexingTopology.common.aggregator.Aggregator;
import indexingTopology.common.data.DataSchema;
import indexingTopology.common.data.PartialQueryResult;
import indexingTopology.common.logics.DataTupleEquivalentPredicateHint;
import indexingTopology.common.logics.DataTuplePredicate;
import indexingTopology.common.logics.DataTupleSorter;
import indexingTopology.config.TopologyConfig;
import indexingTopology.common.Query;
import indexingTopology.util.taxi.City;
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

/**
 * Created by acelzj on 6/19/17.
 */
public class NetworkTemporalQueryCoordinatorWithQueryReceiverServerBolt<T extends Number & Comparable<T>> extends QueryCoordinatorBolt<T> {

    private final int port;

    AtomicLong queryId;

    Server server;

    Map<Long, LinkedBlockingQueue<PartialQueryResult>> queryIdToPartialQueryResults;


//    Map<Long, Semaphore> queryIdToPartialQueryResultSemphore;

    private static final Logger LOG = LoggerFactory.getLogger(GeoTemporalQueryCoordinatorBoltBolt.class);

    public NetworkTemporalQueryCoordinatorWithQueryReceiverServerBolt(T lowerBound, T upperBound, int port,
                                                                      TopologyConfig config, DataSchema schema) {
        super(lowerBound, upperBound, config, schema);
        this.port = port;
    }

    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        super.prepare(map, topologyContext, outputCollector);
        queryId = new AtomicLong(0);
        queryIdToPartialQueryResults = new HashMap<>();


        server = new Server(port, GeoTemporalQueryCoordinatorBoltBolt.QueryServerHandle.class, new Class[]{LinkedBlockingQueue.class, AtomicLong.class, Map.class, City.class, DataSchema.class}, pendingQueue, queryId, queryIdToPartialQueryResults, schema);
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

    static public class QueryServerHandle<T extends Number & Comparable<T>> extends ServerHandle implements QueryHandle, NetworkTemporalQueryHandle {

        LinkedBlockingQueue<List<Query<T>>> pendingQueryQueue;
        AtomicLong queryIdGenerator;
        AtomicLong superQueryIdGenerator;
        DataSchema schema;
        Map<Long, LinkedBlockingQueue<PartialQueryResult>> queryresults;
        public QueryServerHandle(LinkedBlockingQueue<List<Query<T>>> pendingQueryQueue, AtomicLong queryIdGenerator,
                                 Map<Long, LinkedBlockingQueue<PartialQueryResult>> queryresults,
                                 DataSchema schema) {
            this.pendingQueryQueue = pendingQueryQueue;
            this.queryresults = queryresults;
            this.queryIdGenerator = queryIdGenerator;
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
                        request.endTime, request.predicate, request.aggregator, request.sorter, request.equivalentPredicate));
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
        public void handle(NetworkTemporalQueryRequest clientQueryRequest) throws IOException {
            try {
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
                final Aggregator aggregator = clientQueryRequest.aggregator;
                final DataTupleSorter sorter = clientQueryRequest.sorter;
                final DataTupleEquivalentPredicateHint equivalentPredicate = clientQueryRequest.equivalentPredicate;


                queryList.add(new Query(queryid, clientQueryRequest.destIpLowerBound, clientQueryRequest.destIpUpperBound, startTimeStamp, endTimeStamp,
                        predicate, aggregator, sorter, equivalentPredicate));
                LOG.info("A new Query{} ({}, {}, {}, {}) is added to the pending queue.", queryid,
                        clientQueryRequest.destIpLowerBound, clientQueryRequest.destIpUpperBound, startTimeStamp, endTimeStamp);


                pendingQueryQueue.put(queryList);

//                System.out.println("Admitted a query.  waiting for query results");
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
