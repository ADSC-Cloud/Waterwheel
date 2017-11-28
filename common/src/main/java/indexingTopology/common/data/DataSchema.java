package indexingTopology.common.data;

import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;

import java.io.Serializable;
import java.math.BigDecimal;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

/**
 * Created by parijatmazumdar on 17/09/15.
 */
public class DataSchema implements Serializable {

    public static class DataType implements Serializable{
        DataType(Class type, int length) {
            this.type = type;
            this.length = length;
        }



        Object readFromString(String string) {
            if (type.equals(Integer.class)) {
                return Integer.parseInt(string);
            }else if(type.equals(Double.class)) {
                return Double.parseDouble(string);
            }else if(type.equals(Long.class)) {
                return Long.parseLong(string);
            }else if(type.equals(Float.class)) {
                return Float.parseFloat(string);
            }else if(type.equals(Byte.class)) {
                return Byte.parseByte(string);
            }else if(type.equals(Short.class)) {
                return Short.parseShort(string);
            }else if(type.equals(String.class)) {
                return string;
            }
            return null;

        }
        public Class type;
        public int length;
    }

    Object readFromStringByColumn(String string, Class type) {
        if (type.equals(Integer.class)) {
            return Integer.parseInt(string);
        }else if(type.equals(Double.class)) {
            return Double.parseDouble(string);
        }else if(type.equals(Long.class)) {
            return Long.parseLong(string);
        }else if(type.equals(Float.class)) {
            return Float.parseFloat(string);
        }else if(type.equals(Byte.class)) {
            return Byte.parseByte(string);
        }else if(type.equals(Short.class)) {
            return Short.parseShort(string);
        }
        return null;

    }




    public DataSchema(){};

    private int tupleLength = 0;

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

    private final Map<String, Integer> dataFieldNameToIndex = new HashMap<>();
    private final List<String> fieldNames = new ArrayList<>();
    private final List<DataType> dataTypes = new ArrayList<>();
    private String indexField;

    public void setPrimaryIndexField(String name) {
        indexField = name;
    }

    public void addDoubleField(String name) {
        final DataType dataType = new DataType(Double.class, Double.BYTES);
        dataFieldNameToIndex.put(name, fieldNames.size());
        fieldNames.add(name);
        dataTypes.add(dataType);
    }

    public void addFloatField(String name) {
        final DataType dataType = new DataType(Float.class, Float.BYTES);
        dataFieldNameToIndex.put(name, fieldNames.size());
        fieldNames.add(name);
        dataTypes.add(dataType);
    }

    public void addByteField(String name) {
        final DataType dataType = new DataType(Byte.class, Byte.BYTES);
        dataFieldNameToIndex.put(name, fieldNames.size());
        fieldNames.add(name);
        dataTypes.add(dataType);
    }

    public void addIntField(String name) {
        final DataType dataType = new DataType(Integer.class, Integer.BYTES);
        dataFieldNameToIndex.put(name, fieldNames.size());
        fieldNames.add(name);
        dataTypes.add(dataType);
    }

    public void addShortField(String name) {
        final DataType dataType = new DataType(Short.class, Short.BYTES);
        dataFieldNameToIndex.put(name, fieldNames.size());
        fieldNames.add(name);
        dataTypes.add(dataType);
    }

    public void addField(DataType dataType, String fieldName) {
        dataFieldNameToIndex.put(fieldName, fieldNames.size());
        fieldNames.add(fieldName);
        dataTypes.add(dataType);
    }

    public void addVarcharField(String name, int length) {
        final DataType dataType = new DataType(String.class, length);
        dataFieldNameToIndex.put(name, fieldNames.size());
        fieldNames.add(name);
        dataTypes.add(dataType);
    }

    public void addLongField(String name) {
        final DataType dataType = new DataType(Long.class, Long.BYTES);
        dataFieldNameToIndex.put(name, fieldNames.size());
        fieldNames.add(name);
        dataTypes.add(dataType);
    }

    public byte[] serializeTuple(DataTuple t) {
        Output output = new Output(1000, 2000000);
        for (int i = 0; i < dataTypes.size(); i++) {
            if (dataTypes.get(i).type.equals(Double.class)) {
                output.writeDouble((double)t.get(i));
            } else if (dataTypes.get(i).type.equals(String.class)) {
//                output.writeString((String)t.get(i));
                byte[] bytes = ((String) t.get(i)).getBytes();
                output.writeInt(bytes.length);
                output.write(bytes);
            } else if (dataTypes.get(i).type.equals(Integer.class)) {
                output.writeInt((int)t.get(i));
            } else if (dataTypes.get(i).type.equals(Long.class)) {
                output.writeLong((long)t.get(i));
            } else if (dataTypes.get(i).type.equals(Float.class)) {
                output.writeFloat((float)t.get(i));
            } else if (dataTypes.get(i).type.equals(Byte.class)) {
                output.writeByte((byte)t.get(i));
            } else if (dataTypes.get(i).type.equals(Short.class)) {
                output.writeByte((short)t.get(i));
            } else {
                throw new RuntimeException("Not supported data type!" );
            }
        }
        byte[] bytes = output.toBytes();
        output.close();
        return bytes;
    }

    public byte[] dataColumnSerializeTupe(DataTuple t, Class type) {
        Output output = new Output(1000, 2000000);
        for (int i = 0; i < t.size(); i++) {
            if (type.equals(Double.class)) {
                output.writeDouble((double)t.get(i));
            } else if (type.equals(String.class)) {
//                output.writeString((String)t.get(i));
                byte[] bytes = ((String) t.get(i)).getBytes();
                output.writeInt(bytes.length);
                output.write(bytes);
            } else if (type.equals(Integer.class)) {
                output.writeInt((int)t.get(i));
            } else if (type.equals(Long.class)) {
                output.writeLong((long)t.get(i));
            } else if (type.equals(Float.class)) {
                output.writeFloat((float)t.get(i));
            } else if (type.equals(Byte.class)) {
                output.writeByte((byte)t.get(i));
            } else if (type.equals(Short.class)) {
                output.writeByte((short)t.get(i));
            } else {
                throw new RuntimeException("Not supported data type!" );
            }
        }
        byte[] bytes = output.toBytes();
        output.close();
        return bytes;

    }

    public byte[] dataComSerializeTuple(DataTuple t) {
        Output output = new Output(1000, 2000000);
        int j = 0;
        for (int i = 0; i < dataTypes.size(); i++) {
            //System.out.println(fieldNames.get(i));
            if(IsBooleanOrNot.judg(fieldNames.get(i)))
                continue;

            if (dataTypes.get(i).type.equals(Double.class)) {
                output.writeDouble((double)t.get(j));
            } else if (dataTypes.get(i).type.equals(String.class)) {
//                output.writeString((String)t.get(j));
                byte[] bytes = ((String) t.get(j)).getBytes();
                output.writeInt(bytes.length);
                output.write(bytes);
            } else if (dataTypes.get(i).type.equals(Integer.class)) {
                output.writeInt((int)t.get(j));
            } else if (dataTypes.get(i).type.equals(Long.class)) {
                output.writeLong((long)t.get(j));
            } else if (dataTypes.get(i).type.equals(Float.class)) {
                output.writeFloat((float)t.get(j));
            } else if (dataTypes.get(i).type.equals(Byte.class)) {
                output.writeByte((byte)t.get(j));
            } else if (dataTypes.get(i).type.equals(Short.class)) {
                output.writeByte((short)t.get(j));
            } else {
                throw new RuntimeException("Not supported data type!" );
            }
            j++;
        }
        output.writeShort((short)t.get(j));
        byte[] bytes = output.toBytes();
        output.close();
        return bytes;
    }

    public DataTuple deserializeToDataTuple(byte[] b) {
        DataTuple dataTuple = new DataTuple();
        Input input = new Input(b);
        for (int i = 0; i < dataTypes.size(); i++) {

            if (dataTypes.get(i).type.equals(Double.class)) {
                dataTuple.add(input.readDouble());
            } else if (dataTypes.get(i).type.equals(String.class)) {
//                dataTuple.add(input.readString());
                int length = input.readInt();
                byte[] bytes = input.readBytes(length);
                dataTuple.add(new String(bytes));
//                dataTuple.add(input.re);
            } else if (dataTypes.get(i).type.equals(Integer.class)) {
                dataTuple.add(input.readInt());
            } else if (dataTypes.get(i).type.equals(Long.class)) {
                dataTuple.add(input.readLong());
            } else if (dataTypes.get(i).type.equals(Short.class)) {
                dataTuple.add(input.readShort());
            } else {
                throw new RuntimeException("Only classes supported till now are string and double");
            }
        }
        return dataTuple;
    }

    public String getIndexField() {
        return indexField;
    }

    public String getFieldName(int index) {
        return fieldNames.get(index);
    }

    public DataType getDataType(String name) {
        final int offset = dataFieldNameToIndex.get(name);
        return dataTypes.get(offset);
    }

    public DataType getDataType(int index) {
        return dataTypes.get(index);
    }

    public DataType getIndexType() {
        return getDataType(indexField);
    }

    public int getFieldIndex(String fieldName) {
        return dataFieldNameToIndex.get(fieldName);
    }

    public int getNumberOfFields() {
        return dataFieldNameToIndex.size();
    }

    public DataSchema duplicate() {
        DataSchema ret = new DataSchema();
        ret.dataTypes.addAll(dataTypes);
        ret.indexField = indexField;
        ret.dataFieldNameToIndex.putAll(dataFieldNameToIndex);
        ret.fieldNames.addAll(fieldNames);
        return ret;
    }

    public Object getValue(String fieldName, DataTuple dataTuple) {
        final int offset = dataFieldNameToIndex.get(fieldName);
        return dataTuple.get(offset);
    }

    public Object getIndexValue(DataTuple dataTuple) {
        final int indexOffset = dataFieldNameToIndex.get(indexField);
        return dataTuple.get(indexOffset);
    }

    public int getTupleLength() {
        for (int i = 0; i < dataTypes.size(); ++i) {
            tupleLength += dataTypes.get(i).length;
        }
        return tupleLength;
    }

    public String toString() {
        String ret = "";
        for (int i = 0; i < dataTypes.size(); i++) {
            ret += String.format("%s, %s, %d bytes\n", fieldNames.get(i), dataTypes.get(i).type.toString(),
                    dataTypes.get(i).length);
        }
        ret += String.format("index field: %s\n", indexField);
        return ret;
    }

    public List<String> getFieldNames() {
        return fieldNames;
    }


    public boolean equals(Object object) {
        if (!(object instanceof DataSchema))
            return false;
        DataSchema schema = (DataSchema) object;
        if (schema.dataTypes.size() != this.dataTypes.size())
            return false;
        for (int i = 0; i < this.dataTypes.size(); i++) {
            if (!dataTypes.get(i).equals(this.dataTypes.get(i)))
                return false;
            if (!fieldNames.get(i).equals(this.fieldNames.get(i)))
                return false;
        }

        if (this.indexField != null && schema.indexField !=null && ! this.indexField.equals(schema.indexField))
            return false;

        return true;
    }

    public DataTuple parseTupleColumn(String tuple, String split, Class type) throws ParseException {
        String[] attributes = tuple.split(split);
        DataTuple dataTuple = new DataTuple();
        StringBuffer stringBuffer = new StringBuffer();
        Object attribute = null;
        for (int i = 0; i < attributes.length; i++) {
            if(type.equals(Long.class)) {
                Long lg = readLongfromString(attributes[i]);
                attribute = readFromStringByColumn(String.valueOf(lg), type);
            } else if(type.equals(Float.class)) {
                attribute = readFromStringByColumn(attributes[i], type);
            }else if(type.equals(Double.class)) {
                attribute = readFromStringByColumn(attributes[i], type);
            }else if(type.equals(Byte.class)) {
                attribute = readFromStringByColumn(attributes[i], type);
            }else if(type.equals(Short.class)) {
                attribute = readFromStringByColumn(attributes[i], type);
            }
            dataTuple.add(attribute);
        }
        return dataTuple;
    }

    public DataTuple parseJsonTuple(String tuple, String split) throws ParseException {
        String[] attributes = tuple.split(split);
        DataTuple dataTuple = new DataTuple();
        StringBuffer stringBuffer = new StringBuffer();
        Object attribute;
        for (int i = 0; i < attributes.length; i++) {
            if(i == 8){
                Long lg = readLongfromJsonString(attributes[8]);
                attribute = dataTypes.get(i).readFromString(String.valueOf(lg));
            }else{
                if(IsBooleanOrNot.judg(fieldNames.get(i))) {
                    stringBuffer.append(attributes[i]);
                    continue;
                } else if(IsFloatOrNot.judgFloatWithOne(fieldNames.get(i))) {
                    attribute = saveFloatWithOneAsShort(attributes[i]);
                } else if(IsFloatOrNot.judgFloatWithTwo(fieldNames.get(i))) {
                    attribute = saveFloatWithTwoAsShort(attributes[i]);
                } else if(IsFloatOrNot.judgFloatWithOneUnderByte(fieldNames.get(i))) {
                    attribute = saveFloatWithOneUnderByteAsShort(attributes[i]);
                } else {
                    attribute = dataTypes.get(i).readFromString(attributes[i]);
                }
            }

            dataTuple.add(attribute);
        }
        if (stringBuffer.length() != 0){
//            System.out.println("1111");
            attribute = saveBooleanAsShort(stringBuffer);
            dataTuple.add(attribute);
        }
        //System.out.println(dataTuple);
        return dataTuple;
    }

    private static SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
    private static SimpleDateFormat jsonSdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

    public Long readLongfromString(String str) throws ParseException {
        Date data = sdf.parse(str);
        Long l = data.getTime();
        return l;
    }

    public Long readLongfromJsonString(String str) throws ParseException {
        Date data = jsonSdf.parse(str);
        Long l = data.getTime();
        return l;
    }

    static short saveBooleanAsShort(StringBuffer stringBuffer) {
        StringBuffer strBuffer = new StringBuffer();
        for(int i = 14; i >= 0; i--){
            strBuffer.append(stringBuffer.charAt(i));

        }
        return Short.valueOf(strBuffer.toString(),2);
    }

    public static void main(String[] args) {
        System.out.println(saveFloatWithOneUnderByteAsShort("3.1"));
    }
    static short saveFloatWithOneAsShort(String string){
        Float fl = Float.parseFloat(string);
        int Scale = 1;
        int roundingMode = 4;
        BigDecimal bd = new BigDecimal(fl);
        bd = bd.setScale(Scale,roundingMode);
        return (short)(bd.floatValue()*10);
    }

    static short saveFloatWithTwoAsShort(String string){
        Float fl = Float.parseFloat(string);
        int Scale = 2;
        int roundingMode = 4;
        BigDecimal bd = new BigDecimal(fl);
        bd = bd.setScale(Scale,roundingMode);
        return (short)(bd.floatValue()*100);
    }

    static byte saveFloatWithOneUnderByteAsShort(String string){
        Float fl = Float.parseFloat(string);
        int Scale = 1;
        int roundingMode = 4;
        BigDecimal bd = new BigDecimal(fl);
        bd = bd.setScale(Scale,roundingMode);
        return (byte)(bd.floatValue()*10);
    }
}
