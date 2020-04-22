package org.jgroups.util;

import java.util.concurrent.RejectedExecutionHandler;
import java.util.concurrent.ThreadPoolExecutor;

/**
 * // TODO: Document this
 *
 * @author Radim Vansa &lt;rvansa@redhat.com&gt;
 * @since 1/29/13
 */
public class CustomRejectionPolicy implements RejectedExecutionHandler {
    public final static String NAME = "custom";

    private final RejectedExecutionHandler custom;

    public CustomRejectionPolicy(String rejection_policy) {
        if (!rejection_policy.toLowerCase().startsWith("custom=")) {
            throw new IllegalStateException(rejection_policy);
        }
        String className = rejection_policy.substring(7);
        try {
            Class<?> policyClass = Util.loadClass(className, Util.class);
            Object policy = policyClass.getDeclaredConstructor().newInstance();
            if (!(policy instanceof RejectedExecutionHandler)) {
                throw new IllegalArgumentException(className + " does not implement RejectedExecutionHandler");
            } else {
                custom = (RejectedExecutionHandler) policy;
            }
        } catch (Throwable e) {
            throw new RuntimeException("Cannot instantiate rejection policy '" + rejection_policy + "'", e);
        }
    }

    @Override
    public void rejectedExecution(Runnable r, ThreadPoolExecutor executor) {
        custom.rejectedExecution(r, executor);
    }
}
