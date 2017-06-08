package indexingTopology.common.data;

import java.io.Serializable;
import java.util.ArrayList;

/**
 * Created by robert on 9/2/17.
 */
public class DataTuple extends ArrayList<Object> implements Serializable {
    public DataTuple() {
    }

    public DataTuple(DataTuple that) {
        this.addAll(that);
    }

    public DataTuple(Comparable... fields) {
        super(fields.length);
        Object[] objects = fields;
        int length = fields.length;

        for(int i = 0; i < length; ++i) {
            Object o = objects[i];
            this.add(o);
        }
    }

    public boolean equals(Object object) {
        if (! (object instanceof DataTuple))
            return false;
        DataTuple tuple = (DataTuple) object;
        if (tuple.size() != this.size())
            return false;
        for (int i = 0; i < this.size(); i++) {
            if (!this.get(i).equals(tuple.get(i)))
                return false;
        }
        return true;
    }

    public String toDataTypes() {
        String ret = "";
        for (Object object: this) {
            ret += object.getClass().toString() + " ";
        }
        return ret;
    }
}
