package indexingTopology.Cache;

import indexingTopology.util.BTree;

/**
 * Created by acelzj on 11/29/16.
 */
public class TemplateCacheData implements CacheData {

    private BTree template;

    public TemplateCacheData(BTree template) {
        this.template = template;
    }

    public BTree getData() {
        return template;
    }
}
