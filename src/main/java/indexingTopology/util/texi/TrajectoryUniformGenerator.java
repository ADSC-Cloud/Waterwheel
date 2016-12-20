package indexingTopology.util.texi;

import java.util.Random;

/**
 * Created by robert on 19/12/16.
 */
public class TrajectoryUniformGenerator implements TrajectoryGenerator {
    private int numberOfCars;
    private double x1, x2, y1, y2;
    private Random random = new Random();
    private int count = 0;
    public TrajectoryUniformGenerator(int cars, double x1, double x2, double y1, double y2) {
        this.numberOfCars = cars;
        this.x1 = x1;
        this.x2 = x2;
        this.y1 = y1;
        this.y2 = y2;
    }

    public Car generate() {
        double x = x1 + (x2 - x1) * random.nextDouble();
        double y = y1 + (y2 - y1) * random.nextDouble();
        int carId = count;
        count = (count + 1) % numberOfCars;
        return new Car(carId, x, y);
    }

    static public void main(String[] args) {
        TrajectoryUniformGenerator generator = new TrajectoryUniformGenerator(100, 500, 1000, 10000, 20000);
        for (int i = 0; i < 100; i++) {
            System.out.println(generator.generate());
        }
    }





}
