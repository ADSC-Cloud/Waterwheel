package indexingTopology.util.shape;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import indexingTopology.common.data.DataSchema;
import indexingTopology.common.data.DataTuple;
import indexingTopology.util.shape.JsonTuplesToSchemaTuples;
import indexingTopology.util.shape.JudgContain;
import junit.framework.TestCase;
import org.junit.Test;

import java.io.FileReader;
import java.util.List;

/**
 * Create by zelin on 17-11-28
 **/
public class ContainOrNotTest extends TestCase {

    @Test
    public void testJsonTupleToSchemaTuple() throws Exception {
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

        JsonParser parser = new JsonParser();
        JsonObject objectOut =  (JsonObject) parser.parse(new FileReader("/home/hadoop/mygit/append-only-store/topology/src/main/java/indexingTopology/util/shape/test.json"));
        List<DataTuple> list = (List<DataTuple>) JsonTuplesToSchemaTuples.jsonToSchema(outDataSchema, objectOut, "out");
        assertEquals(2, list.size());
    }

    @Test
    public void testJudgShapeContain() throws Exception {
        DataSchema dataSchema = new DataSchema();
        dataSchema.addVarcharField("type", 8);
        dataSchema.addVarcharField("leftTop",15);
        dataSchema.addVarcharField("rightBottom", 15);
        dataSchema.addVarcharField("geoStr", 30);
        dataSchema.addVarcharField("longitude", 10);
        dataSchema.addVarcharField("latitude", 10);
        dataSchema.addVarcharField("radius", 10);

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

        JsonParser parser = new JsonParser();
        JsonObject objectRectangle = (JsonObject) parser.parse(new FileReader("/home/hadoop/mygit/append-only-store/topology/src/main/java/indexingTopology/util/shape/rectangle.json"));
        JsonObject objectPolygon = (JsonObject) parser.parse(new FileReader("/home/hadoop/mygit/append-only-store/topology/src/main/java/indexingTopology/util/shape/polygon.json"));
        JsonObject objectCircle = (JsonObject) parser.parse(new FileReader("/home/hadoop/mygit/append-only-store/topology/src/main/java/indexingTopology/util/shape/circul"));

        JsonObject objectTestCircleOut =  (JsonObject) parser.parse(new FileReader("/home/hadoop/mygit/append-only-store/topology/src/main/java/indexingTopology/util/shape/testcircle.json"));
        JsonObject objectTestPolygonOut =  (JsonObject) parser.parse(new FileReader("/home/hadoop/mygit/append-only-store/topology/src/main/java/indexingTopology/util/shape/testpolygon.json"));
        JsonObject objectTestRectangleOut =  (JsonObject) parser.parse(new FileReader("/home/hadoop/mygit/append-only-store/topology/src/main/java/indexingTopology/util/shape/testrectangle.json"));
        List<DataTuple> outTestCircleList = (List<DataTuple>) JsonTuplesToSchemaTuples.jsonToSchema(outDataSchema, objectTestCircleOut, "out");
        List<DataTuple> outTestPolygonList = (List<DataTuple>) JsonTuplesToSchemaTuples.jsonToSchema(outDataSchema, objectTestPolygonOut, "out");
        List<DataTuple> outTestRectangleList = (List<DataTuple>) JsonTuplesToSchemaTuples.jsonToSchema(outDataSchema, objectTestRectangleOut, "out");
        assertEquals(2, outTestCircleList.size());
        assertEquals(2, outTestPolygonList.size());
        assertEquals(2, outTestRectangleList.size());

        DataTuple dataTupleCircle = (DataTuple) JsonTuplesToSchemaTuples.jsonToSchema(dataSchema, objectCircle, "circle");
        DataTuple dataTuplePolygon = (DataTuple) JsonTuplesToSchemaTuples.jsonToSchema(dataSchema, objectPolygon, "polygon");
        DataTuple dataTupleRectangle = (DataTuple) JsonTuplesToSchemaTuples.jsonToSchema(dataSchema, objectRectangle, "rectangle");

        List<DataTuple> outCircleList = JudgContain.checkInCircle(outTestCircleList,dataTupleCircle);
        assertEquals(1, outCircleList.size());
        List<DataTuple> outPolygonList = JudgContain.checkInPolygon(outTestPolygonList,dataTuplePolygon);
        assertEquals(1, outPolygonList.size());
        List<DataTuple> outRectangleList = JudgContain.checkInRect(outTestRectangleList,dataTupleRectangle);
        assertEquals(1, outRectangleList.size());
    }
}
