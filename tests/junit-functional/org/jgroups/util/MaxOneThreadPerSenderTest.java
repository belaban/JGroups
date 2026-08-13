package org.jgroups.util;

import org.jgroups.EmptyMessage;
import org.jgroups.Global;
import org.jgroups.Message;
import org.jgroups.logging.Log;
import org.jgroups.protocols.UDP;
import org.testng.annotations.Test;

import java.lang.reflect.Proxy;

/**
 * Tests {@link MaxOneThreadPerSender}
 * @author Radoslav Husar
 */
@Test(groups=Global.FUNCTIONAL)
public class MaxOneThreadPerSenderTest {

    /**
     * A BatchHandlerLoop which terminates abnormally (here: because logging the failure fails, too) must not leave
     * the entry with running=true, or else messages from that sender are queued forever and never delivered
     */
    public void testRunningIsResetOnAbnormalTermination() throws Exception {
        UDP tp=new UDP() {
            @Override public void passBatchUp(MessageBatch b, boolean cluster_name_matching, boolean discard_own_mcast) {
                throw new IllegalStateException("failed passing batch up");
            }
        };
        MaxOneThreadPerSender policy=new MaxOneThreadPerSender();
        tp.getThreadPool().init();
        try {
            policy.init(tp);
            policy.log=throwingLog();
            Message msg=new EmptyMessage(null).setSrc(Util.createRandomAddress("A"));
            assert policy.process(msg, false);
            MaxOneThreadPerSender.Entry entry=policy.mcasts.map.values().iterator().next();
            assert Util.waitUntilTrue(5000, 100, () -> !running(entry)) : "entry is still marked as running";
        }
        finally {
            tp.getThreadPool().destroy();
        }
    }

    protected static boolean running(MaxOneThreadPerSender.Entry entry) {
        entry.lock.lock();
        try {
            return entry.running;
        }
        finally {
            entry.lock.unlock();
        }
    }

    /** Returns a Log which fails when an error is logged, e.g. as an OOME would */
    protected static Log throwingLog() {
        return (Log)Proxy.newProxyInstance(MaxOneThreadPerSenderTest.class.getClassLoader(), new Class<?>[]{Log.class},
                                           (proxy, method, args) -> {
                                               if(method.getName().equals("error"))
                                                   throw new IllegalStateException("logging failed");
                                               return method.getReturnType() == boolean.class? Boolean.FALSE : null;
                                           });
    }
}
