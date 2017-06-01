package indexingTopology.topology.kingbase;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created by robert on 22/5/17.
 */
public class TestClass {

    public static void main(String[] args) {
        Number a = null;
        Double b = null;
        Double result = a == null ? b : (a.doubleValue());
        System.out.println(result);
    }

    static private boolean test1() {
        System.out.println("Test 1");
        return true;
    }

    static private boolean test2() {
        System.out.println("Test 2");
        return true;
    }
}
