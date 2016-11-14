package indexingTopology.util;

import javafx.util.Pair;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Created by acelzj on 11/7/16.
 */
public class KeyRangeRecorder <TKey extends Comparable<TKey>,TValue> {

    private ConcurrentHashMap<String, Pair<TKey, TKey>> fileNameToKeyRangeRecorder;

    public KeyRangeRecorder() {
        fileNameToKeyRangeRecorder = new ConcurrentHashMap<String, Pair<TKey, TKey>>();
    }

    public void addKeyRangeToFile(String fileName, TKey minKey, TKey maxKey) {
        fileNameToKeyRangeRecorder.put(fileName, new Pair(minKey, maxKey));
    }

    public List<String> getFileContainingKey(TKey key) {
        List<String> files = new ArrayList<String>();
        for (String fileName : fileNameToKeyRangeRecorder.keySet()) {
            if (key.compareTo(fileNameToKeyRangeRecorder.get(fileName).getKey()) >= 0) {
                if (key.compareTo(fileNameToKeyRangeRecorder.get(fileName).getValue()) <= 0) {
                    files.add(fileName);
                }
            }
        }
        return files;
    }
}
