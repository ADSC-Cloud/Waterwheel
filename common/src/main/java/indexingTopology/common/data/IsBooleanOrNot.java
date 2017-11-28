package indexingTopology.common.data;

/**
 * Create by zelin on 17-8-25
 **/
public class IsBooleanOrNot {
    public enum BooleanFieldName {driver_status, neutralg, reverseg, driveg, climbing_mode,
        high_V_switch, capacity_contactor, capacity_charge_switch, battery_contactor,
        battery_charge_switch, aircon, charge_signal, FDJwork, ebraking, statusFlag2}

    public static boolean judg(String str){
        for (BooleanFieldName fieldName : BooleanFieldName.values()) {
            if(str.equals(fieldName.toString()))
                return true;
        }
        return false;
    }

    public static void main(String[] args) {
        System.out.println(judg("ebaking"));
    }
}
