package indexingTopology.util;

import indexingTopology.FileSystemHandler.FileSystemHandler;
import indexingTopology.FileSystemHandler.LocalFileSystemHandler;

import java.io.*;
import java.nio.ByteBuffer;

/**
 * Created by acelzj on 7/27/16.
 */
public class testReference {
        int a;
        public testReference() {
            a = 2;
        }

        public void setA() {
            a = 3;
        }

        public int getA() {
            return a;
        }
    public static void main(String[] args) {
        testReference apple = new testReference();
        testReference pear = apple;
        apple.setA();
        System.out.println(apple.getA());
        System.out.println(pear.getA());

        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        for (int i = 0; i < 64; ++i) {
            byte [] b = ByteBuffer.allocate(Integer.SIZE / Byte.SIZE).putInt(i).array();
            writeToByteArrayOutputStream(bos, b);
        }

        byte[] bytes = bos.toByteArray();

        int size = bytes.length;


        File file = new File("src/test/test_time");
        FileOutputStream fop = null;

        try {
            fop = new FileOutputStream(file);
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }

        try {
            fop.write(bytes);
        } catch (IOException e) {
            e.printStackTrace();
        }

        RandomAccessFile randomAccessFile = null;
        try {
            randomAccessFile = new RandomAccessFile("src/test/test_time", "r");
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }

        bytes = new byte[size / 4];
        Long totalTime = (long) 0;
        long position = 0;
        for (int i = 0 ; i  < 4; ++i) {
            Long startTime = System.nanoTime();
            try {
                randomAccessFile.read(bytes);
                position += size / 4;
                randomAccessFile.seek(position);
                bytes = new byte[size/4];
            } catch (IOException e) {
                e.printStackTrace();
            }
            totalTime += System.nanoTime() - startTime;
        }
        System.out.println(totalTime);

    }

    private static void writeToByteArrayOutputStream(ByteArrayOutputStream bos, byte[] b) {
        try {
            bos.write(b);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
