package org.gearman.server;

/**
 * Created with IntelliJ IDEA.
 * User: jewart
 * Date: 11/30/12
 * Time: 4:07 PM
 * To change this template use File | Settings | File Templates.
 */

class CountLock {
    private int count = 0;
    private final Runnable task;

    public CountLock(Runnable task) {
        this.task = task;
    }

    public synchronized void reset() {
        count=0;
    }

    public synchronized void lock() {
        count++;
    }

    public boolean isLocked() {
        return count>0;
    }

    public synchronized void unlock() {
        count--;
        runIfNotLocked();
    }

    public synchronized void runIfNotLocked() {
        if(count==0) task.run();
    }
}
