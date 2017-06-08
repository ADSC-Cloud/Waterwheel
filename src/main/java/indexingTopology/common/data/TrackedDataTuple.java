package indexingTopology.common.data;

/**
 * Created by Robert on 5/23/17.
 */
public class TrackedDataTuple extends DataTuple {

    public Long tupleId;
    public int sourceTaskId;

    public TrackedDataTuple(Long tupleId, int sourceTaskId, DataTuple tuple) {
        super(tuple);
        this.tupleId = tupleId;
        this.sourceTaskId = sourceTaskId;
    }
}
