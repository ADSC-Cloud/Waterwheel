package indexingTopology.util.shape;

import indexingTopology.common.data.DataTuple;

import java.util.ArrayList;
import java.util.List;

/**
 * Create by zelin on 17-11-22
 **/
public class TupleModel {
    private String success;
    private List<ResultOne> results;
    private String errorCode;
    private String errorMsg;

    public TupleModel(List<DataTuple> tuples) {
        this.success = "1";
        results = new ArrayList<>();

        for (DataTuple tuple : tuples) {
            ResultOne resultOne = new ResultOne();
            initResult(resultOne, tuple);
            results.add(resultOne);
        }

        this.errorCode = "null";
        this.errorMsg = "null";
    }

    public String getSuccess() {
        return success;
    }

    public List<ResultOne> getResults() {
        return results;
    }

    public String getErrorCode() {
        return errorCode;
    }

    public String getErrorMsg() {
        return errorMsg;
    }

    private static void initResult(ResultOne resultOne, DataTuple tuple) {
        resultOne.setDevbtype(tuple.get(0).toString());
        resultOne.setDevstype(tuple.get(1).toString());
        resultOne.setDevid(tuple.get(2).toString());
        resultOne.setCity(tuple.get(3).toString());
        resultOne.setLongitude(tuple.get(4).toString());
        resultOne.setAltitude(tuple.get(5).toString());
        resultOne.setSpeed(tuple.get(6).toString());
        resultOne.setDirection(tuple.get(7).toString());
        resultOne.setLocationtime(tuple.get(8).toString());
        resultOne.setWorkstate(tuple.get(9).toString());
        resultOne.setClzl(tuple.get(10).toString());
        resultOne.setHphm(tuple.get(11).toString());
        resultOne.setJzlx(tuple.get(12).toString());
        resultOne.setJybh(tuple.get(13).toString());
        resultOne.setJymc(tuple.get(14).toString());
        resultOne.setLxdh(tuple.get(15).toString());
        resultOne.setDth(tuple.get(16).toString());
        resultOne.setReservel(tuple.get(17).toString());
        resultOne.setReservel2(tuple.get(18).toString());
        resultOne.setReservel3(tuple.get(19).toString());
        resultOne.setSsdwdm(tuple.get(20).toString());
        resultOne.setSsdwmc(tuple.get(21).toString());
        resultOne.setTeamno(tuple.get(22).toString());


    }
}
