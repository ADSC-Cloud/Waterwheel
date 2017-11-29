package indexingTopology.common.data;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import net.sf.json.JSONObject;
import org.junit.Test;

import java.io.FileNotFoundException;
import java.io.FileReader;
import java.util.List;

import static junit.framework.TestCase.assertEquals;
import static junit.framework.TestCase.assertTrue;

/**
 * Created by robert on 8/2/17.
 */
public class DataSchemaTest {



    @Test
    public void SchemaTest() {
        DataSchema schema = new DataSchema();
        schema.addDoubleField("double");
        schema.addIntField("int");
        schema.setPrimaryIndexField("double");
        assertEquals(schema.getNumberOfFields(), 2);
    }

    @Test
    public void SerializationTest() {
        DataSchema schema = new DataSchema();
        schema.addDoubleField("f1");
        schema.addLongField("f2");
        DataTuple dataTuple = new DataTuple(0.01, 10L);
        byte[] bytes = schema.serializeTuple(dataTuple);
        DataTuple dataTupleDeserialized = schema.deserializeToDataTuple(bytes);
        assertEquals(0.01, dataTupleDeserialized.get(0));
        assertEquals(10L, dataTupleDeserialized.get(1));
    }

    @Test
    public void SerializationTest1() {
        DataSchema schema = new DataSchema();
        schema.addDoubleField("f1");
        schema.addLongField("f2");
        schema.addVarcharField("f3",3);
        schema.addVarcharField("f4", 2);
        DataTuple dataTuple = new DataTuple(0.01, 10L, "aaaaa", "bbbbbb");
        byte[] bytes = schema.serializeTuple(dataTuple);
        DataTuple dataTupleDeserialized = schema.deserializeToDataTuple(bytes);
        assertEquals(0.01, dataTupleDeserialized.get(0));
        assertEquals(10L, dataTupleDeserialized.get(1));
        assertEquals("aaaaa", dataTupleDeserialized.get(2));
        assertEquals("bbbbbb", dataTupleDeserialized.get(3));
    }

    @Test
    public void IndexFieldTest() {
        DataSchema schema = new DataSchema();
        schema.addDoubleField("f1");
        schema.addLongField("f2");
        schema.setPrimaryIndexField("f2");
        assertEquals("f2", schema.getIndexField());
        assertTrue(schema.getIndexType().type.equals(Long.class));

        DataTuple dataTuple = new DataTuple(0.01, 10L);

        assertEquals(0.01, schema.getValue("f1", dataTuple));

        assertEquals(10L, schema.getIndexValue(dataTuple));
    }

    @Test
    public void getTupleLength() {
        DataSchema schema = new DataSchema();
        schema.addDoubleField("f1");
        schema.addLongField("f2");
        schema.setPrimaryIndexField("f2");

        assertEquals( 16, schema.getTupleLength());
        assertEquals(8, schema.getIndexType().length);
    }

    @Test
    public void getFiledNames() {
        DataSchema schema = new DataSchema();
        schema.addDoubleField("f1");
        schema.addLongField("f2");
        assertEquals("[f1, f2]", schema.getFieldNames().toString());
    }

    @Test
    public void getTuplesFromJsonTest() throws FileNotFoundException {
        DataSchema outDataSchema = init();
        JsonParser parser = new JsonParser();
        JsonObject objectOut =  (JsonObject) parser.parse(new FileReader("jsonfile/testjson.json"));
        List<DataTuple> list =  outDataSchema.getTuplesFromJson(objectOut, "result");
        assertEquals(2, list.size());
    }

    @Test
    public void getTupleFromJsonAndJsonFromTupleTest() throws FileNotFoundException {
        DataSchema outDataSchema = init();
        JsonParser parser = new JsonParser();
        JsonObject objectOut =  (JsonObject) parser.parse(new FileReader("jsonfile/testonejson.json"));
        DataTuple tuple = outDataSchema.getTupleFromJson(objectOut);
        assertEquals(1,tuple.get(0));
        JSONObject jsonObject = outDataSchema.getJsonFromDataTuple(tuple);
        assertEquals("1", jsonObject.get("devbtype"));
    }


    public DataSchema init() {
        DataSchema outDataSchema = new DataSchema();
        outDataSchema.addIntField("devbtype");
        outDataSchema.addVarcharField("devstype", 4);
        outDataSchema.addVarcharField("devid", 6);
        outDataSchema.addIntField("city");
        outDataSchema.addDoubleField("longitude");
        outDataSchema.addDoubleField("latitude");
        outDataSchema.addDoubleField("speed");
        outDataSchema.addDoubleField("direction");
        outDataSchema.addLongField("locationtime");
        outDataSchema.addIntField("workstate");
        outDataSchema.addVarcharField("clzl",4);
        outDataSchema.addVarcharField("hphm", 7);
        outDataSchema.addIntField("jzlx");
        outDataSchema.addLongField("jybh");
        outDataSchema.addVarcharField("jymc", 3);
        outDataSchema.addVarcharField("lxdh", 11);
        outDataSchema.addVarcharField("dth", 12);
        outDataSchema.addIntField("reservel");
        outDataSchema.addIntField("reservel2");
        outDataSchema.addIntField("reservel3");
        outDataSchema.addVarcharField("ssdwdm",12);
        outDataSchema.addVarcharField("ssdwmc", 5);
        outDataSchema.addVarcharField("teamno", 8);
        return outDataSchema;
    }
}
