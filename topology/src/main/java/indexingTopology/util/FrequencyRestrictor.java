package indexingTopology.util;

import org.apache.storm.utils.Utils;

import java.util.concurrent.Semaphore;

/**
 * Created by acelzj on 30/3/17.
 */
public class FrequencyRestrictor {

    int maxFrequencyPerSecond;
    Semaphore semaphore = new Semaphore(0);
    int frequencyPerWindow;
    int millisecondPerWindows;
    int windowsPerSecond;
    Thread permitingThread;
    public FrequencyRestrictor(int maxFrequencyPerSecond, int windowsPerSecond) {
        this.maxFrequencyPerSecond = maxFrequencyPerSecond;
        this.windowsPerSecond = windowsPerSecond;
        verifyParameters();
        this.frequencyPerWindow = maxFrequencyPerSecond / windowsPerSecond;
        millisecondPerWindows = 1000 / windowsPerSecond;
        permitingThread = new Thread(new Runnable() {
            @Override
            public void run() {
                long lastSleepTime = System.currentTimeMillis();
                long offset = 0;
                long count = 0;
                while(true) {
//                    final long currentTime = System.currentTimeMillis();
//                    Utils.sleep(Math.max(0, millisecondPerWindows - (currentTime - lastSleepTime)));
//                    lastSleepTime = System.currentTimeMillis();
//                    semaphore.release(frequencyPerWindow);

                    try {
                        Thread.sleep(millisecondPerWindows);
                    } catch (InterruptedException e) {
//                        e.printStackTrace();
                        Thread.currentThread().interrupt();
                        break;
                    }
                    final long now = System.currentTimeMillis();
                    semaphore.release(Math.max(0, (int)((now - lastSleepTime) / (double) millisecondPerWindows * frequencyPerWindow)));
                    lastSleepTime = now;
                }
            }
        });
        permitingThread.start();
    }

    public FrequencyRestrictor(int maxFrequencyPerSecond) {
        this(maxFrequencyPerSecond, 50);
    }

    public void getPermission() throws InterruptedException{
        getPermission(1);
    }

    public void getPermission(int numberOfEvents) throws InterruptedException {
        semaphore.acquire(numberOfEvents);
    }

    private void verifyParameters() {
        if(windowsPerSecond > 500 || windowsPerSecond < 1) {
            System.err.println("Wrong number of windowsPerSecond! Default value will be used!");
        }
        windowsPerSecond = Math.max(windowsPerSecond, 1);
        windowsPerSecond = Math.min(windowsPerSecond, 500);
    }

    public void close() {
        permitingThread.interrupt();
    }

}
