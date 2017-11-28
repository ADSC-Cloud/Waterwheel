package indexingTopology.common.data;

/**
 * Create by zelin on 17-8-25
 **/
public class IsFloatOrNot {
    public enum FloatWithOneFieldName {DJClnV, DJClnA, FDJClnA, DJSpeed, engine_speed, NSGYW, engine_target_throttle,
        engine_real_throttle, brake_pedal, a8, instant_gas, battery_total_V, charge_discharge_A, soc,
        tow_pedal}
    public enum FloatWithTwoFieldName {battery_min_mono_V}
    public enum FloatWithOneUnderByte {battery_max_mono_V}

    public static boolean judgFloatWithOne(String str){
        for (FloatWithOneFieldName fieldName : FloatWithOneFieldName.values()) {
            if(str.equals(fieldName.toString()))
                return true;
        }
        return false;
    }

    public static boolean judgFloatWithTwo(String str){
        for (FloatWithTwoFieldName fieldName : FloatWithTwoFieldName.values()) {
            if(str.equals(fieldName.toString()))
                return true;
        }
        return false;
    }
    public static boolean judgFloatWithOneUnderByte(String str){
        for (FloatWithOneUnderByte fieldName : FloatWithOneUnderByte.values()) {
            if(str.equals(fieldName.toString()))
                return true;
        }
        return false;
    }
}
