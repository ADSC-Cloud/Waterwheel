package indexingTopology.bloom;

import com.google.common.hash.Funnel;
import com.google.common.hash.Funnels;
import com.google.common.hash.PrimitiveSink;
import indexingTopology.util.texi.Car;
import indexingTopology.util.texi.City;
import indexingTopology.util.texi.Interval;

import java.nio.charset.Charset;


/**
 * Created by john on 30/4/17.
 */

public class DataFunnel {

    private DataFunnel(){}

    public static Funnel<Integer> getIntegerFunnel() {
        return Funnels.integerFunnel();
    }

    public static Funnel<Long> getLongFunnel() {
        return Funnels.longFunnel();
    }

    public static Funnel<Float> getFloatFunnel() {
        return FloatFunnel.INSTANCE;
    }

    public static Funnel<Double> getDoubleFunnel() {
        return DoubleFunnel.INSTANCE;
    }

    public static Funnel<String> getStringFunnel() {
        return StringFunnel.INSTANCE;
    }

    public static CarFunnel getCarFunnel() {
        return CarFunnel.INSTANCE;
    }

    public static CityFunnel getCityFunnel() {
        return CityFunnel.INSTANCE;
    }

    public static IntervalFunnel getIntervalFunnel() {
        return IntervalFunnel.INSTANCE;
    }
}

enum FloatFunnel implements Funnel<Float> {
    INSTANCE;

    @Override
    public void funnel(Float aFloat, PrimitiveSink primitiveSink) {
        primitiveSink.putFloat(aFloat);
    }
}

enum DoubleFunnel implements Funnel<Double> {
    INSTANCE;

    @Override
    public void funnel(Double aDouble, PrimitiveSink primitiveSink) {
        primitiveSink.putDouble(aDouble);
    }
}

enum StringFunnel implements Funnel<String> {
    INSTANCE;

    @Override
    public void funnel(String aString, PrimitiveSink primitiveSink) {
        primitiveSink.putString(aString, Charset.defaultCharset());
    }
}

enum CarFunnel implements Funnel<Car> {
    INSTANCE;
    @Override
    public void funnel(Car car, PrimitiveSink primitiveSink) {
        primitiveSink.putLong(car.id);
        primitiveSink.putDouble(car.x);
        primitiveSink.putDouble(car.y);
    }
}

enum CityFunnel implements Funnel<City> {
    INSTANCE;

    @Override
    public void funnel(City city, PrimitiveSink primitiveSink) {

    }
}

enum IntervalFunnel implements Funnel<Interval> {
    INSTANCE;

    @Override
    public void funnel(Interval interval, PrimitiveSink primitiveSink) {
        primitiveSink.putInt(interval.low);
        primitiveSink.putInt(interval.high);
    }
}