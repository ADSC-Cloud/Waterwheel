package indexingTopology.util.taxi;

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
        final double x1 = 0;
        final double x2 = 1000;
        final double y1 = 0;
        final double y2 = 500;
        final int partitions = 100;

        TrajectoryUniformGenerator generator = new TrajectoryUniformGenerator(100, x1, x2, y1, y2);
        City city = new City(x1, x2, y1, y2, partitions);

        System.out.println(String.format("Max ZCode: %d", city.getMaxZCode()));

        System.out.println(String.format("ZCode: %d", city.getZCodeForALocation(2000,1000)));

        for (int i = 0; i < 100; i++) {
            final Car car = generator.generate();
            System.out.println(car);
            System.out.println(String.format("ZCode: %d", city.getZCodeForALocation(car.x, car.y)));
        }
    }





}
