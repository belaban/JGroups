package org.jgroups.util;

import java.util.concurrent.RejectedExecutionHandler;
import java.util.concurrent.ThreadPoolExecutor;

/**
 * Detects
 *
 * @author Radim Vansa &lt;rvansa@redhat.com&gt;
 * @since 1/29/13
 */
public class ProgressCheckRejectionPolicy implements RejectedExecutionHandler {
    public final static String NAME = "progress_check";

    /** Not changed count of executed tasks for this period will trigger an exception */
    private long period = 10000;

    private long last_completed = -1;
    private long last_change = 0;

    private RejectedExecutionHandler fallback = null;

    public ProgressCheckRejectionPolicy(String rejection_policy) {
        String policy = rejection_policy.toLowerCase();
        if (!policy.startsWith(NAME)) {
            throw new IllegalStateException(rejection_policy);
        }
        policy = policy.substring(NAME.length());
        if (policy.startsWith("=")) {
            String[] attributes = policy.substring(1).split(",", 0);
            for (String attribute : attributes) {
                String[] parts = attribute.split(":");
                if (parts.length != 2) {
                    throw new IllegalArgumentException("Attribute '" + attribute + "' in " + rejection_policy);
                }
                String key = parts[0].trim();
                String value = parts[1].trim();
                if (key.equals("period")) {
                    try {
                        period = Long.parseLong(value);
                    } catch (NumberFormatException e) {
                        throw new IllegalArgumentException("Cannot parse period value in " + rejection_policy, e);
                    }
                } else if (key.equals("fallback")) {
                    // in order to be able to define also different policy with attributes (use as the last attribute)
                    fallback = Util.parseRejectionPolicy(rejection_policy.substring(rejection_policy.indexOf("fallback:") + 9));
                }
            }
        }
    }

    @Override
    public synchronized void rejectedExecution(Runnable r, ThreadPoolExecutor executor) {
        long completed = executor.getCompletedTaskCount();
        if (completed < last_completed) {
            throw new IllegalStateException("Number of completed tasks shouldn't decrease");
        } else if (completed == last_completed) {
            long now = System.currentTimeMillis();
            if (now - last_change > period) {
                String message = String.format(
                        "No progress for %d ms, possible distributed deadlock. Try raising threadpool size\n" +
                        "\tMin size: %d\n\tMax size: %d\n\tCurrent size: %d\n\tActive: %d\n\tLargest size: %d\n" +
                        "\tCompleted tasks: %d\n\tTotal scheduled: %d",
                        now - last_change, executor.getCorePoolSize(), executor.getMaximumPoolSize(),
                        executor.getPoolSize(), executor.getActiveCount(), executor.getLargestPoolSize(),
                        executor.getCompletedTaskCount(), executor.getTaskCount());
                throw new NoProgressException(message);
            }
        } else {
            last_change = System.currentTimeMillis();
            last_completed = completed;
        }
        if (fallback != null) {
            fallback.rejectedExecution(r, executor);
        }
    }
}
