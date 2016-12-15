package indexingTopology;

import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.List;

/**
 * Created by parijatmazumdar on 17/09/15.
 */
public class DataSchema implements Serializable {
    private final Fields dataFields;
    private final List<Class> valueTypes;
    private final String indexField;

    private class SerializationIntermediate {

    }

    public DataSchema(List<String> fieldNames,List<Class> valueTypes, String indexField) {
        assert fieldNames.size()==valueTypes.size() : "number of fields should be " +
                "same as the number of value types provided";
        dataFields=new Fields(fieldNames);
        this.valueTypes=valueTypes;
        this.indexField = indexField;
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
        if (dataFields.size() != valuesAsString.length) throw new IOException("number of values provided does not " +
                "match number of fields in data schema");

        Values values = new Values();
        for (int i=0;i < valueTypes.size();i++) {
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

    public Values deserialize(byte [] b) throws IOException {
        Values values = new Values();
        int offset = 0;
        for (int i = 0; i < valueTypes.size(); i++) {
            if (valueTypes.get(i).equals(Double.class)) {
                int len = Double.SIZE/Byte.SIZE;
                double val = ByteBuffer.wrap(b, offset, len).getDouble();
                values.add(val);
                offset += len;
            } else if (valueTypes.get(i).equals(String.class)) {
                int len = Integer.SIZE/Byte.SIZE;
                int sizeHeader = ByteBuffer.wrap(b, offset, len).getInt();
                offset += len;
                len = sizeHeader;
                String val = new String(b, offset, len);
                values.add(val);
                offset += len;
            } else {
                throw new IOException("Only classes supported till now are string and double");
            }
        }

        int len = Long.SIZE / Byte.SIZE;
        Long val = ByteBuffer.wrap(b, offset, len).getLong();
        values.add(val);

        return values;
    }

    public byte[] serializeTuple(Tuple t) throws IOException {
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        for (int i=0;i<valueTypes.size();i++) {
            if (valueTypes.get(i).equals(Double.class)) {
                byte [] b = ByteBuffer.allocate(Double.SIZE / Byte.SIZE).putDouble(t.getDouble(i)).array();
                bos.write(b);
            } else if (valueTypes.get(i).equals(String.class)) {
                byte [] b = t.getString(i).getBytes();
                byte [] sizeHeader = ByteBuffer.allocate(Integer.SIZE/ Byte.SIZE).putInt(b.length).array();
                bos.write(sizeHeader);
                bos.write(b);
            } else {
                throw new IOException("Only classes supported till now are string and double");
            }
        }

        //As we add timestamp for a field, so we need to serialize the timestamp
        byte [] b = ByteBuffer.allocate(Long.SIZE / Byte.SIZE).putDouble(t.getLong(valueTypes.size())).array();
        bos.write(b);

        return bos.toByteArray();
    }

    public String getIndexField() {
        return indexField;
    }
}
