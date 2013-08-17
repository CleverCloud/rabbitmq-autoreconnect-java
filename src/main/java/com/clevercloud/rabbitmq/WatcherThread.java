package com.clevercloud.rabbitmq;

/**
 * @author Marc-Antoine Perennou<Marc-Antoine@Perennou.com>
 */

public class WatcherThread extends Thread {

   private Watchable watchable;
   private long interval;

   public WatcherThread(Watchable watchable, long interval) {
      this.watchable = watchable;
      this.interval = interval;
   }

   @Override
   public void run() {
      while (true) {
         this.watchable.watch();
         try {
            Thread.sleep(this.interval);
         } catch (InterruptedException ignored) {
         }
      }
   }
}
