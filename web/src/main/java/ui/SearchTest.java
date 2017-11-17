package ui;

import config.Config;
import indexingTopology.api.client.GeoTemporalQueryClient;
import indexingTopology.api.client.GeoTemporalQueryRequest;
import indexingTopology.api.client.QueryResponse;
import indexingTopology.common.aggregator.AggregateField;
import indexingTopology.common.aggregator.Aggregator;
import indexingTopology.common.aggregator.Count;
import indexingTopology.common.data.DataSchema;
import indexingTopology.common.data.DataTuple;
import indexingTopology.common.logics.DataTupleEquivalentPredicateHint;
import indexingTopology.common.logics.DataTuplePredicate;
import model.DataBean;

import java.io.IOException;
import java.net.SocketTimeoutException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.List;
import java.util.Random;

public class SearchTest {

    private double Selectivity = 1;

    private String QueryServerIp = Config.ServerHost;

    private int NumberOfQueries = Integer.MAX_VALUE;

    private int get_xLow = 0,
            get_xHigh = 0,
            get_yLow = 0,
            get_yHigh = 0;


    static final double x1 = 40.012928;
    static final double x2 = 40.023983;
    static final double y1 = 116.292677;
    static final double y2 = 116.614865;
    static final int partitions = 128;

    private int RecentSecondsOfInterest = 5;

    DataBean dataBean;

    public SearchTest(int get_xLow, int get_xHigh, int get_yLow, int get_yHigh, int recentSecondsOfInterest) {
        this.get_xLow = get_xLow;
        this.get_xHigh = Math.max(get_xLow, get_xHigh);
        this.get_yLow = get_yLow;
        this.get_yHigh = Math.max(get_yLow, get_yHigh);
        RecentSecondsOfInterest = recentSecondsOfInterest;
    }

    public DataBean executeQuery() {

        double selectivityOnOneDimension = Math.sqrt(Selectivity);
        DataSchema schema = getDataSchema();
        GeoTemporalQueryClient queryClient = new GeoTemporalQueryClient(QueryServerIp, 10001);
        dataBean = new DataBean();
        try {
            queryClient.connectWithTimeout(10000);
        } catch (IOException e) {
            e.printStackTrace();
        }
        Random random = new Random();

        int executed = 0;

        double xLow = x1 + (x2 - x1) * get_xLow / 1000.0;
        double xHigh = x1 + (x2 - x1) * get_xHigh / 1000.0;
        double yLow = y1 + (y2 - y1) * get_yLow / 1000.0;
        double yHigh = y1 + (y2 - y1) * get_yHigh / 1000.0;
        if(RecentSecondsOfInterest == 0)
            RecentSecondsOfInterest = 5;

//                DataTuplePredicate predicate = t ->
//                                 (double) schema.getValue("lon", t) >= xLow &&
//                                (double) schema.getValue("lon", t) <= xHigh &&
//                                (double) schema.getValue("lat", t) >= yLow &&
//                                (double) schema.getValue("lat", t) <= yHigh ;

        final int id = new Random().nextInt(100000);
        final String idString = "" + id;
        DataTuplePredicate predicate = t -> schema.getValue("id", t).equals(idString);



        Aggregator<Integer> aggregator = new Aggregator<>(schema, "id", new AggregateField(new Count(), "*"));


        DataTupleEquivalentPredicateHint equivalentPredicateHint = new DataTupleEquivalentPredicateHint("id", idString);

        GeoTemporalQueryRequest queryRequest = new GeoTemporalQueryRequest<>(xLow, xHigh, yLow, yHigh,
                System.currentTimeMillis() - RecentSecondsOfInterest * 1000,
                System.currentTimeMillis(), null,null, null, null);
        long start = System.currentTimeMillis();
        try {
            DateFormat dateFormat = new SimpleDateFormat("MM-dd HH:mm:ss");
            Calendar cal = Calendar.getInstance();
            System.out.println("[" + dateFormat.format(cal.getTime()) + "]: A query will be issued.");
            QueryResponse response = queryClient.query(queryRequest);
            System.out.println("A query finished.");
            long end = System.currentTimeMillis();

            DataSchema outputSchema = response.getSchema();
            System.out.println("this is fieldNames" + outputSchema.getFieldNames());
            List<DataTuple> tuples = response.getTuples();
            dataBean.setTuples(tuples);
            dataBean.setFieldNames(outputSchema.getFieldNames());
            dataBean.setTime(end - start);
            System.out.println(outputSchema.getFieldNames().size());

            System.out.println(String.format("Query time: %d ms", end - start));



        } catch (SocketTimeoutException e) {
            Thread.interrupted();
        } catch (IOException e) {
            if (Thread.currentThread().interrupted()) {
                Thread.interrupted();
            }
            e.printStackTrace();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }


        try {
            queryClient.close();
        } catch (IOException e) {
            e.printStackTrace();
        }

        return  dataBean;
    }

    static private DataSchema getDataSchema() {
        DataSchema schema = new DataSchema();
        schema.addVarcharField("id", 32);
        schema.addVarcharField("veh_no", 10);
        schema.addDoubleField("lon");
        schema.addDoubleField("lat");
        schema.addIntField("car_status");
        schema.addDoubleField("speed");
        schema.addVarcharField("position_type", 10);
        schema.addVarcharField("update_time", 32);
        schema.addIntField("zcode");
        schema.addLongField("timestamp");
        schema.setPrimaryIndexField("zcode");
        return schema;
    }

    public static void main(String[] args) {
        SearchTest searchTest1 = new SearchTest(0,0,0,0,0);
        DataBean dataBean = searchTest1.executeQuery();
        System.out.println("This is FieldNamesSize" + dataBean.getFieldNames().size());
    }
}
