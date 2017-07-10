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

    public void setTag(String key, String value) {
        tags.put(key, value);
    }

    public String getTag(String key) {
        return tags.get(key);
    }

    public String toString() {
        return tags.toString();
    }

    public void merge(Tags tags) {
        if (tags != null)
            this.tags.putAll(tags.tags);
    }
}
