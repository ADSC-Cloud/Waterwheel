package ui;

import indexingTopology.api.client.QueryClient;
import indexingTopology.api.client.QueryRequest;
import indexingTopology.api.client.QueryResponse;
import indexingTopology.common.aggregator.AggregateField;
import indexingTopology.common.aggregator.Aggregator;
import indexingTopology.common.aggregator.Count;
import indexingTopology.common.data.DataSchema;
import indexingTopology.common.data.DataTuple;
import indexingTopology.common.logics.DataTupleEquivalentPredicateHint;
import indexingTopology.common.logics.DataTuplePredicate;
import model.DataBean;

import javax.ws.rs.OPTIONS;
import java.io.IOException;
import java.net.SocketTimeoutException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.List;
import java.util.Random;

/**
 * @Create By Hzl
 * @Date 2017/8/7 10:50
 */
public class SearchTest {

    private String QueryServerIp = "localhost";

    static final int keys = 1024 * 1024;
    static final int x1 = 0, x2 = keys - 1;
    static final int payloadSize = 100;

    private double Selectivity = 1;

    private int RecentSecondsOfInterest = 5;

    private int NumberOfQueries = Integer.MAX_VALUE;

    DataBean dataBean = new DataBean();

    public DataBean executeQuery() throws IOException {

        DataSchema schema = null;
        QueryClient queryClient = new QueryClient(QueryServerIp, 10001);
        try {
            queryClient.connectWithTimeout(10000);
            schema = queryClient.querySchema();
        } catch (IOException e) {
            e.printStackTrace();
            return null;
        }
        Random random = new Random();

        int executed = 0;
        long totalQueryTime = 0;


            int x = (int) (x1 + (x2 - x1) * (1 - Selectivity) * random.nextDouble());

            final int xLow = x;
            final int xHigh = (int) (x + Selectivity * (x2 - x1));

//                DataTuplePredicate predicate = t ->
//                                 (double) schema.getValue("lon", t) >= xLow &&
//                                (double) schema.getValue("lon", t) <= xHigh &&
//                                (double) schema.getValue("lat", t) >= yLow &&
//                                (double) schema.getValue("lat", t) <= yHigh ;

            final int id = new Random().nextInt(keys);
            final String idString = "" + id;
//                DataTuplePredicate predicate = t -> schema.getValue("id", t).equals(Integer.toString(new Random().nextInt(100000)));
            DataTuplePredicate predicate = null;
//                DataTuplePredicate predicate = t -> schema.getValue("id", t).equals(idString);


            Aggregator<Integer> aggregator = new Aggregator<>(schema, "id", new AggregateField(new Count(), "*"));
//                Aggregator<Integer> aggregator = null;


//                DataSchema schemaAfterAggregation = aggregator.getOutputDataSchema();
//                DataTupleSorter sorter = (DataTuple o1, DataTuple o2) -> Double.compare((double) schemaAfterAggregation.getValue("count(*)", o1),
//                        (double) schemaAfterAggregation.getValue("count(*)", o2));


//                DataTupleEquivalentPredicateHint equivalentPredicateHint = new DataTupleEquivalentPredicateHint("id", idString);
            DataTupleEquivalentPredicateHint equivalentPredicateHint = null;

            QueryRequest<Integer> queryRequest = new QueryRequest<>(xLow, xHigh,
                    System.currentTimeMillis() - RecentSecondsOfInterest * 1000,
                    System.currentTimeMillis(), predicate, aggregator, null, equivalentPredicateHint);
            long start = System.currentTimeMillis();
            try {
                DateFormat dateFormat = new SimpleDateFormat("MM-dd HH:mm:ss");
                Calendar cal = Calendar.getInstance();
                System.out.println("[" + dateFormat.format(cal.getTime()) + "]: A query will be issued.");
                QueryResponse response = queryClient.query(queryRequest);
                System.out.println("A query finished.");
                long end = System.currentTimeMillis();
                totalQueryTime += end - start;
                DataSchema outputSchema = response.getSchema();
                System.out.println("this si FieldName:" + outputSchema.getFieldNames());
                List<DataTuple> tuples = response.getTuples();
                dataBean.setTuples(tuples);
                dataBean.setFieldNames(outputSchema.getFieldNames());
                for (int i = 0; i < tuples.size(); i++) {
                    System.out.println(tuples.get(i).toValues());
                }
                System.out.println(String.format("Query time [%d]: %d ms", executed, end - start));

                if (++executed >= NumberOfQueries) {
                    System.out.println("Average Query Latency: " + totalQueryTime / (double) executed);

                }

            } catch (SocketTimeoutException e) {
                Thread.interrupted();
            } catch (IOException e) {
                if (Thread.currentThread().interrupted()) {
                    Thread.interrupted();
                }
                e.printStackTrace();
            }


        try {
            queryClient.close();
        } catch (IOException e) {
            e.printStackTrace();
        }

        return dataBean;
    }

    public static void main(String[] args) throws IOException {
        SearchTest  searchTest = new SearchTest();
        searchTest.executeQuery();
    }
}
