package indexingTopology.bolt;

import indexingTopology.client.*;
import indexingTopology.data.PartialQueryResult;
import indexingTopology.util.Query;
import indexingTopology.util.texi.ZOrderCoding;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
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

    private ZOrderCoding zOrderCoding;

    private static final Logger LOG = LoggerFactory.getLogger(KingBaseQueryCoordinatorWithQueryReceiverServer.class);

    public KingBaseQueryCoordinatorWithQueryReceiverServer(T lowerBound, T upperBound, int port, ZOrderCoding zOrderCoding) {
        super(lowerBound, upperBound);
        this.port = port;
        this.zOrderCoding = zOrderCoding;
    }

    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        super.prepare(map, topologyContext, outputCollector);
        queryId = new AtomicLong(0);
        queryIdToPartialQueryResults = new HashMap<>();


        server = new Server(port, QueryServerHandle.class, new Class[]{LinkedBlockingQueue.class, AtomicLong.class, Map.class, ZOrderCoding.class}, pendingQueue, queryId, queryIdToPartialQueryResults, zOrderCoding);
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
    }

    @Override
    public void handlePartialQueryResult(Long queryId, PartialQueryResult partialQueryResult) {
        LinkedBlockingQueue<PartialQueryResult> results = queryIdToPartialQueryResults.computeIfAbsent(queryId, k -> new LinkedBlockingQueue<>());

        try {
            results.put(partialQueryResult);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    static public class QueryServerHandle extends ServerHandle implements QueryHandle {

        LinkedBlockingQueue<Query> pendingQueryQueue;
        AtomicLong queryIdGenerator;
        AtomicLong superQueryIdGenerator;
        Map<Long, LinkedBlockingQueue<PartialQueryResult>> queryresults;
        ZOrderCoding zOrderCoding;
        public QueryServerHandle(LinkedBlockingQueue<Query> pendingQueryQueue, AtomicLong queryIdGenerator,
                                 Map<Long, LinkedBlockingQueue<PartialQueryResult>> queryresults,
                                 ZOrderCoding zOrderCoding) {
            this.pendingQueryQueue = pendingQueryQueue;
            this.queryresults = queryresults;
            this.queryIdGenerator = queryIdGenerator;
            this.zOrderCoding = zOrderCoding;
        }

        @Override
        public void handle(QueryRequest request) throws IOException {
            try {
                final long queryid = queryIdGenerator.getAndIncrement();
                final long superQueryId = superQueryIdGenerator.getAndIncrement();

//                zOrderCoding.
//                request.low

                LinkedBlockingQueue<PartialQueryResult> results =
                        queryresults.computeIfAbsent(queryid, k -> new LinkedBlockingQueue<>());

                LOG.info("A new Query{} ({}, {}, {}, {}) is added to the pending queue.", queryid,
                        request.low, request.high, request.startTime, request.endTime);
                pendingQueryQueue.put(new Query<>(queryid, request.low, request.high, request.startTime,
                        request.endTime, request.predicate, request.aggregator));

                boolean eof = false;
                while(!eof) {
                    PartialQueryResult partialQueryResult = results.take();
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
