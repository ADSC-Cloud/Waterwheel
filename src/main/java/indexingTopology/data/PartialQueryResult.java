package indexingTopology.data;



import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by robert on 3/3/17.
 */
public class PartialQueryResult implements Serializable {

    public List<DataTuple> dataTuples = new ArrayList<>();

    // capacity of a PartialQueryResult in number of tuples.
    private int capacity;

    private boolean EOFflag = false;

    public PartialQueryResult(PartialQueryResult partialQueryResult) {
        this.capacity = partialQueryResult.capacity;
        this.EOFflag = partialQueryResult.EOFflag;
        this.dataTuples = partialQueryResult.dataTuples;
    }

    public PartialQueryResult(int capacity) {
        this.capacity = capacity;
    }

    public PartialQueryResult() {
        this(1024);
    }

    public String toString() {
        String ret = "";
        for(DataTuple tuple: dataTuples) {
            ret += tuple + "\n";
        }
        return ret;
    }

    public boolean add(DataTuple tuple) {
        if (dataTuples.size() < capacity) {
            dataTuples.add(tuple);
            return true;
        } else
            return false;
    }

    public boolean isFull() {
        return dataTuples.size() == capacity;
    }


    public int getRemainingSlots() {
        return capacity - dataTuples.size();
    }

    public void setEOFflag() {
        EOFflag = true;
    }

    public boolean getEOFFlag() {
        return EOFflag;
    }

    static public List<PartialQueryResult> Compact(PartialQueryResult result, int capacity) {
        List<PartialQueryResult> results = new ArrayList<>();
        results.add(result);
        return Compact(results, capacity);
    }

    static public List<PartialQueryResult> Compact(List<PartialQueryResult> results, int capacity) {

        List<PartialQueryResult> compactedResults = new ArrayList<>();
        PartialQueryResult currentPartialQueryResult = new PartialQueryResult(capacity);
        while (results != null && results.size() > 0) {
            final int tuplesToMove = Math.min(results.get(0).dataTuples.size(), currentPartialQueryResult.getRemainingSlots());
            currentPartialQueryResult.dataTuples.addAll(results.get(0).dataTuples.subList(0, tuplesToMove));
            results.get(0).dataTuples.subList(0, tuplesToMove).clear();
            if (currentPartialQueryResult.isFull()) {
                compactedResults.add(currentPartialQueryResult);
                currentPartialQueryResult = new PartialQueryResult(capacity);
            }
            if (results.get(0).dataTuples.isEmpty()) {
                results.remove(0);
            }
        }

        if (results != null && !currentPartialQueryResult.dataTuples.isEmpty()) {
            compactedResults.add(currentPartialQueryResult);
        }

        return compactedResults;
    }
}
