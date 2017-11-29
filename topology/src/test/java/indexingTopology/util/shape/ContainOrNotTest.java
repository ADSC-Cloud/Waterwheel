package indexingTopology.util.shape;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import indexingTopology.common.data.DataSchema;
import indexingTopology.common.data.DataTuple;
import junit.framework.TestCase;
import org.junit.Test;
import net.sf.json.JSONObject;

import java.io.FileReader;
import java.util.List;

/**
 * Create by zelin on 17-11-28
 **/
public class ContainOrNotTest extends TestCase {

    @Test
    public void testJudgShapeContain() throws Exception {
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
        JsonObject objectRectangle = (JsonObject) parser.parse(new FileReader("jsonfile/testrectangle.json"));
        JsonObject objectPolygon = (JsonObject) parser.parse(new FileReader("jsonfile/testpolygon.json"));
        JsonObject objectCircle = (JsonObject) parser.parse(new FileReader("jsonfile/testcircle.json"));

        DataTuple tupleRectangle = outDataSchema.getTupleFromJson(objectRectangle);
        DataTuple tuplePolygon = outDataSchema.getTupleFromJson(objectPolygon);
        DataTuple tupleCircle = outDataSchema.getTupleFromJson(objectCircle);

        Point point;

        point = new Point((double)tupleRectangle.get(4), (double)tupleRectangle.get(5));
        boolean isRectangle = new CheckInRectangle().checkIn(point);
        assertEquals(true, isRectangle);

        point = new Point((double)tuplePolygon.get(4), (double)tuplePolygon.get(5));
        boolean isPolygon = new CheckInPolygon().checkIn(point);
        assertEquals(true, isPolygon);

        point = new Point((double)tupleCircle.get(4), (double)tupleCircle.get(5));
        boolean isCircle = new CheckInCircle().checkIn(point);
        assertEquals(true, isCircle);

    }
}
