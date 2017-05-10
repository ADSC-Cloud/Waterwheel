package indexingTopology.bloom;

import com.google.common.hash.Funnel;
import com.google.common.hash.PrimitiveSink;
import java.nio.charset.Charset;

import indexingTopology.util.texi.Car;
import indexingTopology.util.texi.Interval;


/**
 * Created by john on 30/4/17.
 */

public class DataFunnel {

    private DataFunnel(){}

    public static Funnel<Boolean> getBooleanFunnel() {
        return BooleanFunnel.INSTANCE;
    }

    public static Funnel<Character> getCharFunnel() {
        return CharacterFunnel.INSTANCE;
    }

    public static Funnel<Byte> getByteFunnel() {
        return ByteFunnel.INSTANCE;
    }

    public static Funnel<Short> getShortFunnel() {
        return ShortFunnel.INSTANCE;
    }

    public static Funnel<Integer> getIntegerFunnel() {
        return IntegerFunnel.INSTANCE;
    }

    public static Funnel<Long> getLongFunnel() {
        return LongFunnel.INSTANCE;
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

    public static IntervalFunnel getIntervalFunnel() {
        return IntervalFunnel.INSTANCE;
    }
}

enum BooleanFunnel implements Funnel<Boolean> {
    INSTANCE;

    @Override
    public void funnel(Boolean aBoolean, PrimitiveSink primitiveSink) {
        primitiveSink.putBoolean(aBoolean.booleanValue());
    }
}

enum CharacterFunnel implements Funnel<Character> {
    INSTANCE;

    @Override
    public void funnel(Character aChar, PrimitiveSink primitiveSink) {
        primitiveSink.putChar(aChar.charValue());
    }
}

enum ByteFunnel implements Funnel<Byte> {
    INSTANCE;

    @Override
    public void funnel(Byte aByte, PrimitiveSink primitiveSink) {
        primitiveSink.putByte(aByte.byteValue());
    }
}

enum ShortFunnel implements Funnel<Short> {
    INSTANCE;

    @Override
    public void funnel(Short aShort, PrimitiveSink primitiveSink) {
        primitiveSink.putShort(aShort.shortValue());
    }
}

enum IntegerFunnel implements Funnel<Integer> {
    INSTANCE;

    @Override
    public void funnel(Integer aInteger, PrimitiveSink primitiveSink) {
        primitiveSink.putInt(aInteger.intValue());
    }
}

enum LongFunnel implements Funnel<Long> {
    INSTANCE;

    @Override
    public void funnel(Long aLong, PrimitiveSink primitiveSink) {
        primitiveSink.putLong(aLong.longValue());
    }
}

enum FloatFunnel implements Funnel<Float> {
    INSTANCE;

    @Override
    public void funnel(Float aFloat, PrimitiveSink primitiveSink) {
        primitiveSink.putFloat(aFloat.floatValue());
    }
}

enum DoubleFunnel implements Funnel<Double> {
    INSTANCE;

    @Override
    public void funnel(Double aDouble, PrimitiveSink primitiveSink) {
        primitiveSink.putDouble(aDouble.doubleValue());
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

enum IntervalFunnel implements Funnel<Interval> {
    INSTANCE;

    @Override
    public void funnel(Interval interval, PrimitiveSink primitiveSink) {
        primitiveSink.putInt(interval.low);
        primitiveSink.putInt(interval.high);
    }
}