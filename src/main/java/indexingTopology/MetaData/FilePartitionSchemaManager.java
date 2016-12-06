package indexingTopology.MetaData;

import com.github.davidmoten.rtree.Entry;
import com.github.davidmoten.rtree.RTree;
import com.github.davidmoten.rtree.geometry.Geometries;
import com.github.davidmoten.rtree.geometry.Rectangle;
import indexingTopology.MetaData.FileMetaData;
import rx.Observable;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by acelzj on 11/24/16.
 */
public class FilePartitionSchemaManager {

    private RTree<FileMetaData, Rectangle> tree = RTree.create();


    // Add a FileMetaData to the manager
    public void add(FileMetaData fileMetaData) {
        tree = tree.add(fileMetaData, Geometries.rectangle(fileMetaData.keyRangeLowerBound, fileMetaData.startTime,
                fileMetaData.keyRangeUpperBound, fileMetaData.endTime));
    }

    // Retrieve the set of files for a given key range and time duration
    public List<String> search(double keyRangeLowerBound, double keyRangeUpperBound, long startTime,
                                     long endTime) {
        List<String> ret = new ArrayList<String>();
        Observable<Entry<FileMetaData, Rectangle>> result = tree.search(Geometries.rectangle(keyRangeLowerBound,
                startTime, keyRangeUpperBound, endTime));


        for (Entry<FileMetaData, Rectangle> e : result.toBlocking().toIterable()) {
            ret.add(e.value().filename);
        }

        return ret;
    }

    //Retrieve the set of files for a given key range
    public List<String> keyRangedSearch(double keyRangeLowerBound, double keyRangeUpperBound) {
        return search(keyRangeLowerBound, keyRangeUpperBound, Long.MIN_VALUE, Long.MAX_VALUE);
    }

    //Retrieve the set of files for a given time duration
    public List<String> timeRangedSearch(long startTime, long endTime) {
        return search(Double.MIN_VALUE, Double.MAX_VALUE, startTime, endTime);
    }

}
