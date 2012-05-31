package org.jgroups.util;

import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

/**
 * Mock implementation of {@link TimeScheduler}, used by unit tests
 * @author Bela Ban
 * @since 3.0
 */
public class MockTimeScheduler implements TimeScheduler {
    public void execute(Runnable command) {
    }

    public Future<?> schedule(Runnable command, long delay, TimeUnit unit) {
        return null;
    }

    public Future<?> scheduleWithFixedDelay(Runnable command, long initialDelay, long delay, TimeUnit unit) {
        return null;
    }

    public Future<?> scheduleAtFixedRate(Runnable command, long initialDelay, long period, TimeUnit unit) {
        return null;
    }

    public Future<?> scheduleWithDynamicInterval(Task task) {
        return null;
    }

    public void setThreadFactory(ThreadFactory factory) {
    }

    public String dumpTimerTasks() {
        return null;
    }

    public int getMinThreads() {
        return 0;
    }

    public void setMinThreads(int size) {
    }

    public int getMaxThreads() {
        return 0;
    }

    public void setMaxThreads(int size) {
    }

    public long getKeepAliveTime() {
        return 0;
    }

    public void setKeepAliveTime(long time) {
    }

    public int getCurrentThreads() {
        return 0;
    }

    public int size() {
        return 0;
    }

    public void stop() {
    }

    public boolean isShutdown() {
        return false;
    }

}
