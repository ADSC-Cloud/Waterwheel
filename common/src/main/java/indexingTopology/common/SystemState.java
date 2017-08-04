package indexingTopology.common;

import java.util.Map;

import java.io.Serializable;
import java.util.HashMap;

/**
 * Created by billlin on 2017/7/31.
 */
public class SystemState implements Serializable{

    public double throughout;
    public double[] lastThroughput;
    public HashMap<String,String> hashMap;

    public void setHashMap(HashMap<String,String> hashMap){
        this.hashMap = hashMap;
    }
}
