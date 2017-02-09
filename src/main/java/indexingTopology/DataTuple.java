package indexingTopology;

import java.io.Serializable;
import java.util.ArrayList;

/**
 * Created by robert on 9/2/17.
 */
public class DataTuple extends ArrayList<Object> implements Serializable {
    public DataTuple() {
    }

    public DataTuple(Object... fields) {
        super(fields.length);
        Object[] objects = fields;
        int length = fields.length;

        for(int i = 0; i < length; ++i) {
            Object o = objects[i];
            this.add(o);
        }
    }
}
