package indexingTopology.common;

import java.util.Map;

import java.io.Serializable;
import java.util.HashMap;

/**
 * Created by billlin on 2017/7/31.
 */
public class SystemState implements Serializable{

    private double throughout;
    public double[] lastThroughput;
    private HashMap<String,String> hashMap;
    public double getThroughput() {
        return throughout;
    }
    public void setThroughout(double throughout) {
        this.throughout = throughout;
    }
    public double[] getLastThroughput() {
        return lastThroughput;
    }
    public void setLastThroughput(double[] lastThroughput) {
        this.lastThroughput = lastThroughput;
    }
    public void setHashMap(String k,String v){
        if(hashMap == null){
            hashMap = new HashMap<>();
        }
        this.hashMap.put(k,v);
    }
    public HashMap<String,String> getHashMap(){
        return hashMap;
    }
    @Override
    public String toString() {
        return "[throughput=" + throughout + ", lastThroughput=" + lastThroughput + ", hashMap="
                + hashMap + "]";
    }
}
