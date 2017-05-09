package indexingTopology.util;

import indexingTopology.data.DataSchema;
import indexingTopology.data.DataTuple;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

/**
 * Created by robert on 9/5/17.
 */
public interface DataTupleSorter extends Comparator<DataTuple>, Serializable {


    static void main(String[] args) {
        DataSchema schema = new DataSchema();
        schema.addIntField("int");
        schema.addDoubleField("double");
        schema.addVarcharField("string", 100);

        List<DataTuple> tuples = new ArrayList<>();
        tuples.add(new DataTuple(1, 2.5, "c1"));
        tuples.add(new DataTuple(4, 4.5, "b1"));
        tuples.add(new DataTuple(3, 1.5, "ea"));
        tuples.add(new DataTuple(0, -2.6, "eb"));

        DataTupleSorter sorter = new DataTupleSorter() {
            @Override
            public int compare(DataTuple o1, DataTuple o2) {
                return Integer.compare((int)schema.getValue("int", o1),
                        (int)schema.getValue("int", o2));
            }
        };

        Collections.sort(tuples, sorter);
        System.out.println(tuples);

        sorter = new DataTupleSorter() {
            @Override
            public int compare(DataTuple o1, DataTuple o2) {
                return ((String)schema.getValue("string", o1)).compareTo(
                ((String)schema.getValue("string", o2)));
            }
        };

        Collections.sort(tuples, sorter);
        System.out.println(tuples);



    }
}
