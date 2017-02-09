package indexingTopology;

import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by parijatmazumdar on 17/09/15.
 */
public class DataSchema implements Serializable {

    static class DataType {
        DataType(Class type, int length) {
            this.type = type;
            this.length = length;
        }
        Class type;
        int length;
    }

    public DataSchema(){};

    public DataSchema(List<String> fieldNames,List<Class> valueTypes, String indexField) {
        assert fieldNames.size()==valueTypes.size() : "number of fields should be " +
                "same as the number of value types provided";
        for(int i = 0; i < valueTypes.size(); i++) {
            if (valueTypes.get(i).equals(Integer.class)) {
                addIntField(fieldNames.get(i));
            } else if (valueTypes.get(i).equals(Long.class)) {
                addLongField(fieldNames.get(i));
            } else if (valueTypes.get(i).equals(Double.class)) {
                addDoubleField(fieldNames.get(i));
            } else if (valueTypes.get(i).equals(String.class)) {
                throw new RuntimeException("String is not support in the constructor.");
            }
        }
        this.indexField = indexField;
    }

    private final Map<String, DataType> dataFields = new HashMap<>();
    private final List<String> fieldNames = new ArrayList<>();
    private final List<DataType> dataTypes = new ArrayList<>();
    private String indexField;

    public void setPrimaryIndexField(String name) {
        indexField = name;
    }

    public void addDoubleField(String name) {
        final DataType dataType = new DataType(Double.class, Double.BYTES);
        fieldNames.add(name);
        dataTypes.add(dataType);
        dataFields.put(name, dataType);
    }

    public void addIntField(String name) {
        final DataType dataType = new DataType(Integer.class, Integer.BYTES);
        fieldNames.add(name);
        dataTypes.add(dataType);
        dataFields.put(name, dataType);
    }

    public void addVarcharField(String name, int length) {
        final DataType dataType = new DataType(String.class, length);
        fieldNames.add(name);
        dataTypes.add(dataType);
        dataFields.put(name, dataType);
    }

    public void addLongField(String name) {
        final DataType dataType = new DataType(Long.class, Long.BYTES);
        fieldNames.add(name);
        dataTypes.add(dataType);
        dataFields.put(name, dataType);
    }


    public Fields getFieldsObject() {
        return new Fields(fieldNames);
    }

    public Values getValuesObject(String [] valuesAsString) throws RuntimeException {
        if (dataFields.size() != valuesAsString.length) throw new RuntimeException("number of values provided does not " +
                "match number of fields in data schema");

        Values values = new Values();
        for (int i=0;i < dataTypes.size();i++) {
            if (dataTypes.get(i).equals(Double.class)) {
                values.add(Double.parseDouble(valuesAsString[i]));
            }
            else if (dataTypes.get(i).equals(String.class)) {
                values.add(valuesAsString[i]);
            } else if (dataTypes.get(i).equals(Integer.class)) {
                values.add(Integer.parseInt(valuesAsString[i]));
            } else if (dataTypes.get(i).equals(Long.class)) {
                values.add(Long.parseLong(valuesAsString[i]));
            } else {
                throw new RuntimeException("Only classes supported till now are string and double");
            }
        }

        return values;
    }

    /*
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
    */


    /*
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
        byte [] b = ByteBuffer.allocate(Long.SIZE / Byte.SIZE).putLong(t.getLong(valueTypes.size())).array();
        bos.write(b);

        return bos.toByteArray();
    }
    */


    public byte[] serializeTuple(Tuple t) throws IOException {
        Output output = new Output(1000, 2000000);
        for (int i = 0; i < dataTypes.size(); i++) {
            if (dataTypes.get(i).equals(Double.class)) {
                output.writeDouble(t.getDouble(i));
            } else if (dataTypes.get(i).equals(String.class)) {
                output.writeString(t.getString(i));
            } else if (dataTypes.get(i).equals(Integer.class)) {
                output.writeInt(t.getInteger(i));
            } else if (dataTypes.get(i).equals(Long.class)) {
                output.writeLong(t.getLong(i));
            } else {
                throw new IOException("Only classes supported till now are string and double");
            }
        }
        return output.toBytes();
    }


    public Values deserialize(byte [] b) throws IOException {
        Values values = new Values();
        Input input = new Input(b);
        for (int i = 0; i < dataTypes.size(); i++) {
            if (dataTypes.get(i).equals(Double.class)) {
                values.add(input.readDouble());
            } else if (dataTypes.get(i).equals(String.class)) {
                values.add(input.readString());
            } else if (dataTypes.get(i).equals(Integer.class)) {
                values.add(input.readInt());
            } else if (dataTypes.get(i).equals(Long.class)) {
                values.add(input.readLong());
            } else {
                throw new IOException("Only classes supported till now are string and double");
            }
        }

        values.add(input.readLong());
        return values;
    }

    public String getIndexField() {
        return indexField;
    }

    public int getNumberOfFields() {
        return dataFields.size();
    }

    public DataSchema duplicate() {
        DataSchema ret = new DataSchema();
        ret.dataTypes.addAll(dataTypes);
        ret.indexField = indexField;
        ret.dataFields.putAll(dataFields);
        ret.fieldNames.addAll(fieldNames);
        return ret;
    }
}
