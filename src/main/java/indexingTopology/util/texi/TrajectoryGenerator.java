package indexingTopology.util.texi;

import java.io.Serializable;

/**
 * Created by robert on 19/12/16.
 */
public interface TrajectoryGenerator extends Serializable {
    Car generate();
}
