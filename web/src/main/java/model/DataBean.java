package model;

import indexingTopology.common.data.DataTuple;

import java.util.List;

/**
 * @Create By Hzl
 * @Date 2017/8/7 11:21
 */
public class DataBean  {
    List<DataTuple> tuples;
    List<String> fieldNames;
    long time;

    public long getTime() {
        return time;
    }

    public void setTime(long time) {
        this.time = time;
    }

    public List<DataTuple> getTuples() {
        return tuples;
    }

    public List<String> getFieldNames() {
        return fieldNames;
    }

    public void setTuples(List<DataTuple> tuples) {
        this.tuples = tuples;
    }

    public void setFieldNames(List<String> fieldNames) {
        this.fieldNames = fieldNames;
    }
}
