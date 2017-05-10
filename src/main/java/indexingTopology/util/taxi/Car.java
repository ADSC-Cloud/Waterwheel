package indexingTopology.util.taxi;

/**
 * Created by Robert on 12/11/16.
 */
public class Car {
    public long id;
    public double x;
    public double y;
    public Car(int id, double x, double y) {
        this.id = id;
        this.x = x;
        this.y = y;
    }
    public String toString() {
        String string = String.format("Car %d: (%f, %f)", id, x, y);
        return string;
    }
}
