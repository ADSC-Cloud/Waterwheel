package indexingTopology.cache;

import javafx.util.Pair;

/**
 * Created by acelzj on 11/29/16.
 */
public class TemplateCacheData implements CacheData {

    private Pair data;

    public TemplateCacheData(Pair data) {
        this.data = data;
    }

    public Pair getData() {
        return data;
    }
}
