package indexingTopology.util.taxi;

import org.apache.storm.metric.internal.RateTracker;

import java.util.Random;

/**
 * Created by robert on 10/5/17.
 */
public class TrajectoryMovingGenerator implements TrajectoryGenerator {


    private double x1;
    private double x2;
    private double y1;
    private double y2;
    private int numberOfCars;
    private double xCoordinatorMovementPerSecond;
    private double yCoordinatorMovementPerSecond;
    private Random random;

    final double changeDirectEveryNSeconds = 60.0;

    private CarStatus[] cars;

    static class CarStatus {
        public long time;
        public Car car;
        public char direction;
        public CarStatus(Car car, long time, char direction) {
            this.car = car;
            this.time = time;
            this.direction = direction;
        }
    }

    public TrajectoryMovingGenerator(double x1, double x2, double y1, double y2, int numberOfCars, double traverseTimeInMins) {
        if (x1 >= x2 || y1 >= y2)
            throw new IllegalArgumentException(String.format("Illegal Args: %s, %s, %s, %s", x1, x2, y1, y2));
        this.x1 = x1;
        this.x2 = x2;
        this.y1 = y1;
        this.y2 = y2;
        this.numberOfCars = numberOfCars;
        xCoordinatorMovementPerSecond = (x2 - x1) / (traverseTimeInMins * 60);
        yCoordinatorMovementPerSecond = (y2 - y1) / (traverseTimeInMins * 60);
        random = new Random();
        cars = new CarStatus[numberOfCars];
        for (int i = 0; i < numberOfCars; i++) {
            double x = x1 + (x2 - x1) * random.nextDouble();
            double y = y1 + (y2 - y1) * random.nextDouble();
            char direction = randomDirection();
            long time = System.currentTimeMillis();
            cars[i] = new CarStatus(new Car(i, x, y), time, direction);
        }
    }

    @Override
    public Car generate() {
        int index = random.nextInt(cars.length);
        CarStatus carStatus = cars[index];
        final Car car = carStatus.car;
        char direction = carStatus.direction;
        final long timeDurationInMillis = System.currentTimeMillis() - carStatus.time;
        double x = car.x;
        double y = car.y;
        if (random.nextDouble() < timeDurationInMillis / (changeDirectEveryNSeconds * 1000)) {
            direction = randomDirection();
        }
        switch (direction) {
            case 'E':  {
                y -= timeDurationInMillis / 1000 * yCoordinatorMovementPerSecond;
                if (y < y1) {
                    y = y1;
                    direction = 'W';
                }
                break;
            }
            case 'W':  {
                y += timeDurationInMillis / 1000 * yCoordinatorMovementPerSecond;
                if (y > y2) {
                    y = y2;
                    direction = 'E';
                }
                break;
            }
            case 'S':  {
                x -= timeDurationInMillis / 1000 * xCoordinatorMovementPerSecond;
                if (x < x1) {
                    x = x1;
                    direction = 'N';
                }
                break;
            }
            case 'N':  {
                x += timeDurationInMillis / 1000 * xCoordinatorMovementPerSecond;
                if (x > x2) {
                    x = x2;
                    direction = 'S';
                }
                break;
            }
        }
        car.x = x;
        car.y = y;
        carStatus.time = System.currentTimeMillis();
        carStatus.direction = direction;
        return car;
    }

    private char randomDirection() {
        switch (random.nextInt(4)) {
            case 0: return 'E';
            case 1: return 'W';
            case 2: return 'S';
            case 3: return 'N';
            default: return '!';
        }
    }

    public static void main(String[] arsg) throws InterruptedException {
        TrajectoryMovingGenerator generator = new TrajectoryMovingGenerator(40.012928, 40.023983, 116.292677, 116.614865, 10000, 1.0);
        RateTracker rateTracker = new RateTracker(2000,2);
        new Thread(() -> {
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            while(true) {
                System.out.println(String.format("Throughput: %4.4f", rateTracker.reportRate()));
            }
        }).start();
        while (true) {
            Car car = generator.generate();
            rateTracker.notify(1);
        }
    }
}
