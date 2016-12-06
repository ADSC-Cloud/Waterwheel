package indexingTopology.Cache;

import indexingTopology.util.BTreeLeafNode;

/**
 * Created by acelzj on 11/29/16.
 */
public class LeafNodeCacheData implements CacheData {

    private BTreeLeafNode leaf;

    public LeafNodeCacheData(BTreeLeafNode leaf) {
        this.leaf = leaf;
    }

    public BTreeLeafNode getData() {
        return leaf;
    }
}
