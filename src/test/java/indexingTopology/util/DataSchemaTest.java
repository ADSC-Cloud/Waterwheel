package indexingTopology.util;

import indexingTopology.DataSchema;
import org.junit.Test;

import static junit.framework.TestCase.assertEquals;

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
}
