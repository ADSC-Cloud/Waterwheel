package indexingTopology.topology.kingbase;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created by robert on 22/5/17.
 */
public class TestClass {

    public static void main(String[] args) {
        String name = "taskId" + 100 + "chunk" + 32;
        Pattern pattern = Pattern.compile("taskId(\\d+)");
        Matcher matcher = pattern.matcher(name);
//        return matcher.group(1);
        if (matcher.find())
        {
            System.out.println(matcher.group(1));
        }
    }
}
