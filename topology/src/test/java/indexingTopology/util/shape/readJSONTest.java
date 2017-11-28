package indexingTopology.util.shape;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

import java.io.FileReader;


/**
 * Create by zelin on 17-11-16
 **/
public class readJSONTest {
    public static void readJSON() throws Exception{

        // 创建json解析器
        JsonParser parser = new JsonParser();
        // 使用解析器解析json数据，返回值是JsonElement，强制转化为其子类JsonObject类型
        JsonObject object =  (JsonObject) parser.parse(new FileReader("/home/hadoop/mygit/append-only-store/topology/src/test/java/indexingTopology/util/shape/test.json"));

        // 使用JsonObject的get(String memeberName)方法返回JsonElement，再使用JsonElement的getAsXXX方法得到真实类型
        System.out.println("cat = " + object.get("cat").getAsString());

        // 遍历JSON数组
        JsonArray languages = object.getAsJsonArray("languages");
        for (JsonElement jsonElement : languages) {
            JsonObject language = jsonElement.getAsJsonObject();
            System.out.println("id = " + language.get("id").getAsInt() + ",ide = " + language.get("ide").getAsString() + ",name = " + language.get("name").getAsString());
        }

        System.out.println("pop = " + object.get("pop").getAsString());
    }

    public static void main(String[] args) throws Exception {
        readJSONTest.readJSON();
    }
}
