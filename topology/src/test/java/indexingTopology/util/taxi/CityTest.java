package indexingTopology.util.taxi;

import java.util.*;

import junit.framework.TestCase;

/**
 * Created by Robert on 12/11/16.
 */
public class CityTest extends TestCase{

    public void testCityIntervals() {
        City city = new City(0, 100, 0, 200, 10);
        Intervals intervals = city.getZCodeIntervalsInARectagle(20, 30, 80, 100);
        assertEquals("[36,39]", intervals.toString());
    }

    public void testCityCarRetrieveInARectangle() {
        City city = new City(0, 2000, 500, 2234.3, 1000);
        Car car1 = new Car(0, 100, 1024);
        city.store(car1);
        assertTrue(city.getCarsInARectangle(50, 120, 1000, 1200).contains(car1));
    }

    public void testCityCarRetrieveInARectangleRandom() {
        City city = new City(0, 2000, 0, 2000, 500);
        final double x1 = 500;
        final double x2 = 700;
        final double y1 = 1024;
        final double y2 = 1080;
        final int cars = 1000;

        Random random = new Random();

        ArrayList<Car> generatedCars = new ArrayList<>();
        HashSet<Car> carsInRectangle = new HashSet<>();


        for (int i = 0; i < cars; i++) {
            double x = random.nextDouble() * 2000;
            double y = random.nextDouble() * 2000;
            Car car = new Car(i, x, y);
            generatedCars.add(car);
            if(car.x >= x1 && car.x <= x2 && car.y >= y1 && car.y <= y2)
                carsInRectangle.add(car);
        }

        for (Car car: generatedCars) {
            city.store(car);
        }

        List<Car> retrievedCars = city.getCarsInARectangle(x1, x2, y1, y2);

        assertEquals(carsInRectangle.size(), retrievedCars.size());
        for(Car car: retrievedCars) {
            assertTrue(carsInRectangle.contains(car));
        }

    }
}