package indexingTopology.common;

import java.util.Map;

import java.io.Serializable;
import java.util.HashMap;
import java.util.TreeMap;

/**
 * Created by billlin on 2017/7/31.
 */
public class SystemState implements Serializable{

    private double throughout;
    private double cpuRatio;

    private double totalDiskSpaceInGB;
    private double availableDiskSpaceInGB;

    public double[] lastThroughput;
    private HashMap<String,String> hashMap;
    private TreeMap<String,String> treeMap;
    static public int NumberOfHistoricThroughputs = 6;
    public void setCpuRatio(double cpuRatio) {
        this.cpuRatio = cpuRatio;
    }
    public double getRatio() {
        return cpuRatio;
    }
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
    public void changeHashMap(HashMap<String,String> map){
        this.hashMap = map;
    }


    public void setTreeMap(String k,String v){
        if(treeMap == null){
            treeMap = new TreeMap<>();
        }
        this.treeMap.put(k,v);
    }
    public TreeMap<String,String> getTreeMap(){
        return treeMap;
    }
    @Override
    public String toString() {
        return "[throughput=" + throughout + ", lastThroughput=" + lastThroughput + ", hashMap="
                + hashMap + ", cpuRatio="+ cpuRatio + " ,treeMap="+treeMap + "]";
    }

    public double getTotalDiskSpaceInGB() {
        return totalDiskSpaceInGB;
    }

    public void setTotalDiskSpaceInGB(double totalDiskSpaceInGB) {
        this.totalDiskSpaceInGB = totalDiskSpaceInGB;
    }

    public double getAvailableDiskSpaceInGB() {
        return availableDiskSpaceInGB;
    }

    public void setAvailableDiskSpaceInGB(double availableDiskSpaceInGB) {
        this.availableDiskSpaceInGB = availableDiskSpaceInGB;
    }
}
