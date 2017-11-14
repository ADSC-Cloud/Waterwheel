package ui;

//import apple.laf.JRSUIUtils;
import indexingTopology.common.SystemState;

import java.io.PrintWriter;
import java.util.*;

/**
 * Created by billlin on 2017/8/9.
 */
public class asd {
    public static void main(String[] args) {

        int x,y,z;
        x = y = z = 0;
        int a = 2&2&1;
//        System.out.println(a);
        double[] throughputList = new double[6];
        for (int i = 0; i < 6; i++) {
            throughputList[i] = new Random().nextDouble() * 1000;
        }
        SystemState systemState = new SystemState();
        systemState.setThroughout(20.0);
        systemState.setLastThroughput(throughputList);
        systemState.setHashMap("1","/Users/billlin/机器学习asd/分布式/tmp/append-only-store/dataDir");

        systemState.setHashMap("10","10");
        systemState.setHashMap("11","11");
        systemState.setHashMap("12","12");
        systemState.setHashMap("13","13");
        systemState.setHashMap("14","14");
        systemState.setHashMap("15","15");
        systemState.setHashMap("16","16");
        systemState.setHashMap("17","17");
        systemState.setHashMap("18","18");
        systemState.setHashMap("1","1");
        systemState.setHashMap("2","2");
        systemState.setHashMap("3","3");
        systemState.setHashMap("4","4");
        systemState.setHashMap("5","5");
        systemState.setHashMap("6","6");
        systemState.setHashMap("7","7");
        systemState.setHashMap("8","8");
        systemState.setHashMap("9","9");
        systemState.setHashMap("b12","23");
        systemState.setHashMap("b21","23");
        systemState.setHashMap("19","19");
        systemState.setHashMap("20","20");
        systemState.setHashMap("21","21");
        systemState.setHashMap("22","22");
        systemState.setHashMap("23","23");
        systemState.setHashMap("a12","23");
        systemState.setHashMap("a21","23");
        Map<String,String> map = systemState.getHashMap();
        Iterator iter = map.entrySet().iterator();
        List<String> list2 = new ArrayList<String>();
        while (iter.hasNext()) {
            Map.Entry entry = (Map.Entry) iter.next();
            String key = (String) entry.getKey();
            String val = (String)entry.getValue();
            list2.add(key);
        }
//        String source[] = { "dad", "bood", "bada", "Admin", "Aa ", "A ", "Good", "aete", "cc", "Ko", "Beta", "Could","1","22","1"};

//        List<String> list = Arrays.asList(source);

        //String.CASE_INSENSITIVE_ORDER A在 a 前面
//        Collections.sort(list2, String.CASE_INSENSITIVE_ORDER);
//        String[][] source = new String[100][2];
////        List<String> list = Arrays.asList(source);
//        int i=0;
//        Iterator iterBig = map.entrySet().iterator();
//        while (iterBig.hasNext()) {
//            iter = map.entrySet().iterator();
//            while (iter.hasNext()) {
//                Map.Entry entry = (Map.Entry) iter.next();
//                String key = (String) entry.getKey();
//                String val = (String) entry.getValue();
//                if (key.equals(list2.get(i))) {
//                    System.out.println("key: "+key);
//                    source[i][0] = key;
//                    source[i][1] = val;
//                    i++;
//                }
//            }
//        }
//        System.out.println("list2: "+list2);







//



//        Object[]   key   =  map2.keySet().toArray();
//        Arrays.sort(key);
//        for   (int   i   =   0;   i   <   key.length;   i++)   {
//            System.out.println(map2.get(key[i]));
//        }




        HashMap<String, String> map2 = new HashMap<String, String>();
        map2.put("bill", "ccccc");
        map2.put("de", "aaaaa");
        map2.put("TEST", "aaaaa");
        map2.put("Tesy", "aaaaa");
        map2.put("firs", "bbbbb");
        map2.put("Deas", "ddddd");


        HashMap<String, String> maptest =new  HashMap<String, String>();
        maptest.put("De","day1");
        maptest.put("taw","day5");
        maptest.put("T","day4");
        maptest.put("dea","day2");
        maptest.put("v","day3");

//        List<Map.Entry<String,String>> list111 = new ArrayList<Map.Entry<String,String>>(map2.entrySet());
//        Collections.sort(list111,new Comparator<Map.Entry<String,String>>() {
//            //升序排序
//            public int compare(Map.Entry<String, String> o1,
//                               Map.Entry<String, String> o2) {
//                return o1.getValue().compareTo(o2.getValue());
//            }
//
//        });
//
//        for(Map.Entry<String,String> mapping:list111){
//            System.out.println(mapping.getKey()+":"+mapping.getValue());
//        }

        //这里将map.entrySet()转换成list
        List<Map.Entry<String,String>> list222 = new ArrayList<Map.Entry<String,String>>(systemState.getHashMap().entrySet());
        //然后通过比较器来实现排序
        Collections.sort(list222,new Comparator<Map.Entry<String,String>>() {
            //升序排序
            public int compare(Map.Entry<String, String> o1,
                               Map.Entry<String, String> o2) {

                return o1.getKey().toLowerCase().compareTo(o2.getKey().toLowerCase());
            }

        });
        HashMap<String,String> sortHashMap = new HashMap<String,String>();
        for(Map.Entry<String,String> mapping:list222){
            System.out.println("key: "+mapping.getKey()+",value: "+mapping.getValue());
            sortHashMap.put(mapping.getKey(),mapping.getValue());
        }
        Iterator it123 = sortHashMap.entrySet().iterator();
        while (it123.hasNext()) {
            Map.Entry entry = (Map.Entry) it123.next();
            String key = (String) entry.getKey();
            String val = (String)entry.getValue();
            System.out.println(key+" : "+val);
        }


//        for (Iterator it = sortHashMap.keySet().iterator(); it.hasNext();) {
////            Person person = map.get(it.next());
////            System.out.println(person.getId_card() + " " + person.getName());
//            Map.Entry entry = (Map.Entry) it.next();
//            String key = (String) entry.getKey();
//            String val = (String) entry.getValue();
//            System.out.println(key+" : "+val);
//        }

        TreeMap treemap = new TreeMap(map2);
//        Map<String,String> treemap1 = treemap;
        Iterator it = treemap.entrySet().iterator();
        while (it.hasNext()) {
            Map.Entry entry = (Map.Entry) it.next();
            String key = (String) entry.getKey();
            String val = (String)entry.getValue();
            System.out.println(key+" : "+val);
        }
        Collection<String> keyset= systemState.getHashMap().keySet();
        List<String> list = new ArrayList<String>(keyset);

        //对key键值按字典升序排序
        Collections.sort(list,String.CASE_INSENSITIVE_ORDER);

//        for(Map.Entry<String,String> mapping1:list){
//            System.out.println(mapping1.getKey()+":"+mapping1.getValue());
//        }
        for (int i = 0; i < list.size(); i++) {
            System.out.println("key键---值: "+list.get(i)+","+systemState.getHashMap().get(list.get(i)));
        }

        TreeMap<String,String> tree = new TreeMap<String,String>();
        tree.put("bill", "ccccc");
        tree.put("de", "aaaaa");
        tree.put("TEST", "aaaaa");
        tree.put("Tesy", "aaaaa");
        tree.put("firs", "bbbbb");
        tree.put("Deas", "ddddd");
        Iterator treeIt = tree.entrySet().iterator();
        while (treeIt.hasNext()) {
            Map.Entry entry = (Map.Entry) treeIt.next();
            String key = (String) entry.getKey();
            String val = (String)entry.getValue();
            System.out.println(key+" : "+val);
        }
    }
}
