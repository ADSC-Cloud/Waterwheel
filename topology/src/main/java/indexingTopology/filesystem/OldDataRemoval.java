package indexingTopology.filesystem;

import java.io.File;
import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * Created by billlin on 2017/11/14
 */
public class OldDataRemoval {
    String path = "data/";
    File file;
    File file2;

    public void traverFolder(String path){
        File folder = new File(path);
        if(folder.exists()){
            File[] files = folder.listFiles();
            if(files.length == 0){
                System.out.println("Null !");
                return;
            }
            else {
                for(File singleFile : files){
                    if(singleFile.getName().equals(".DS_Store")){
                        continue;
                    }
                    String ctime = new SimpleDateFormat("yyy-MM-dd hh:mm:ss").format(new Date(singleFile.lastModified()));
                    String ntime = new SimpleDateFormat("yyy-MM-dd hh:mm:ss").format(new Date(System.currentTimeMillis()));
                    System.out.println(ctime);
                    System.out.println(ntime);
                    if(System.currentTimeMillis()-singleFile.lastModified() >= 30){
                        singleFile.delete();
                        System.out.printf("success");
                    }
//                    System.out.println(new Date(System.currentTimeMillis()));

//                    singleFile.delete();
//                    remove(singleFile.);
//                    System.out.println(singleFile.getAbsolutePath());
                }
            }
        }
    }

    public void remove(String filePath) throws InterruptedException {
        file = new File("data/test");
        long time = file.lastModified();
        file2 = new File("data/taskId7chunk1");
        long time2 = file2.lastModified();
//        System.out.println(time);
//        System.out.println(new Date(time));
//        Date date = new Date(time);
////        Thread.sleep(1000);
//        Date date2 = new Date(time);
        System.out.println(time-time2);
        String ctime = new SimpleDateFormat("yyy-MM-dd hh:mm:ss").format(new Date(0));
        System.out.println(ctime);
    }

    public static void main(String[] args) throws InterruptedException {
        OldDataRemoval removal = new OldDataRemoval();
//        removal.remove();
        Thread thread = new Thread(()->{
            try {
                Thread.sleep(30000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            removal.traverFolder(removal.path);
        });
        thread.start();
    }
}
