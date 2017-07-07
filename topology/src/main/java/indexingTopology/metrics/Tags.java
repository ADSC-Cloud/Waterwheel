package indexingTopology.metrics;

import java.io.Serializable;
import java.text.Format;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by robert on 7/7/17.
 */
public class Tags implements Serializable {
    Map<String, String> tags = new HashMap<>();

    void setTag(String key, String value) {
        tags.put(key, value);
    }

    String getTag(String key) {
        return tags.get(key);
    }

    public String toString() {
        return tags.toString();
    }
}
