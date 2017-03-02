package indexingTopology;

import org.junit.Test;

import static junit.framework.TestCase.assertEquals;
import static junit.framework.TestCase.assertTrue;
import static org.junit.Assert.*;

/**
 * Created by acelzj on 20/2/17.
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

}