package indexingTopology.util;

import java.util.HashMap;

/**
 * Created by parijatmazumdar on 18/01/16.
 */
public class TimingModule {
    private HashMap<String,Long> time;
    private TimingModule() {
        time = new HashMap<String, Long>();
    }

    public static TimingModule createNew() {
        return new TimingModule();
    }

    public void startTiming(String id) {
        time.put(id,System.nanoTime());
    }

    public void putDuration(String id, long duration) {
        time.put(id,duration);
    }

    public void endTiming(String id) {
        time.put(id,System.nanoTime()-time.get(id));
    }

    public void reset() {
        time.clear();
    }

    public long getTotal() {
        long total = 0;
        for (String k : time.keySet()) {
            total+=time.get(k);
        }

        return total;
    }

    public String printTimes() {
        StringBuffer sb = new StringBuffer();
        int count = time.keySet().size();
        for (String k : time.keySet()) {
            sb.append(k+":"+time.get(k));
            if (count>1)
                sb.append(" , ");

            count--;
        }

        return sb.toString();
    }
}
