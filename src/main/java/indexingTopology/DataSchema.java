package indexingTopology;

import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by parijatmazumdar on 17/09/15.
 */
public class DataSchema implements Serializable {
    private final Fields dataFields;
    private final List<Class> valueTypes;
    public DataSchema(List<String> fieldNames,List<Class> valueTypes) {
        assert fieldNames.size()==valueTypes.size() : "number of fields should be " +
                "same as the number of value types provided";
        dataFields=new Fields(fieldNames);
        this.valueTypes=valueTypes;
    }

    public Fields getFieldsObject() { return dataFields; }
    public Values getValuesObject(Tuple tuple) throws IOException {
        Values values=new Values();
        for (int i=0;i<valueTypes.size();i++) {
            if (valueTypes.get(i).equals(Double.class)) {
                values.add(tuple.getDouble(i));
            }
            else if (valueTypes.get(i).equals(String.class)) {
                values.add(tuple.getString(i));
            }
            else {
                throw new IOException("Only classes supported till now are string and double");
            }
        }

        return values;
    }

    public Values getValuesObject(String [] valuesAsString) throws IOException {
        if (dataFields.size()!=valuesAsString.length) throw new IOException("number of values provided does not " +
                "match number of fields in data schema");

        Values values=new Values();
        for (int i=0;i<valueTypes.size();i++) {
            if (valueTypes.get(i).equals(Double.class)) {
                values.add(Double.parseDouble(valuesAsString[i]));
            }
            else if (valueTypes.get(i).equals(String.class)) {
                values.add(valuesAsString[i]);
            }
            else {
                throw new IOException("Only classes supported till now are string and double");
            }
        }

        return values;
    }
}
