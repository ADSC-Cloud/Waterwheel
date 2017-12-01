package indexingTopology.util.shape;

import java.io.Serializable;

/**
 * Point on 2D landscape
 *
 * @author Roman Kushnarenko (sromku@gmail.com)</br>
 */
public class Point implements Serializable {

    public Point(double x, double y) {
        this.x = x;
        this.y = y;
    }

    public double x;
    public double y;

    @Override
    public String toString() {
        return String.format("(%f,%f)", x, y);
    }
}