package indexingTopology.util.texi;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.function.Predicate;

/**
 * Created by robert on 14/12/16.
 */
public class Database<Key, Value>  implements Serializable {

    private TreeMap<Key, ArrayList<Value>> treeMap = new TreeMap<>();

    public void put(Key key, Value value) {
        if(!treeMap.containsKey(key)) {
            treeMap.put(key, new ArrayList<Value>());
        }
        treeMap.get(key).add(value);
    }

    public Collection<Value> rangeSearch(Key low, Key high) {
        ArrayList<Value> ret = new ArrayList<>();
        SortedMap<Key, ArrayList<Value>> submap = treeMap.subMap(low, true, high, true);
        for (Key key: submap.keySet()) {
            ret.addAll(submap.get(key));
        }
        return ret;
    }

    public Collection<Value> reangeSearch(Key low, Key high, Predicate<Value> predicate) {
        ArrayList<Value> ret = new ArrayList<>();
        SortedMap<Key, ArrayList<Value>> submap = treeMap.subMap(low, true, high, true);
        for (Key key: submap.keySet()) {
            for(Value value: submap.get(key)) {
                if(predicate.test(value))
                    ret.add(value);
            }
        }
        return ret;
    }
}
