package org.jgroups.blocks;

import org.jgroups.Global;
import org.jgroups.JChannel;
import org.jgroups.blocks.locking.LockService;
import org.jgroups.fork.ForkChannel;
import org.jgroups.protocols.CENTRAL_LOCK2;
import org.jgroups.protocols.FORK;
import org.jgroups.stack.Protocol;
import org.jgroups.util.Util;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.Test;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;

@Test(groups= Global.FUNCTIONAL,singleThreaded=true)
public class LockServiceWithCentralLock2Test {
    protected JChannel    c;
    protected ForkChannel fc;
    protected static final String PROPS="fork.xml";
    protected static final String CLUSTER=LockServiceWithCentralLock2Test.class.getSimpleName();

    @AfterMethod protected void destroy() {
        Util.close(fc,c);
    }

    public void shouldLockServiceGetLockUsingForkChannel() throws Exception {
        c = create(PROPS, "A", true).connect(CLUSTER);
        _shouldLockServiceGetLockUsingForkChannel();
    }

    public void shouldLockServiceGetLockUsingForkChannelProgrammaticCreation() throws Exception {
        c = create(null, "A", true).connect(CLUSTER);
        _shouldLockServiceGetLockUsingForkChannel();
    }

    public void shouldRequestHandlerRunningUsingForkChannel() throws Exception {
        c=create(PROPS, "A", true);
        fc = new ForkChannel(c, "lock", "lock-channel");
        _shouldRequestHandlerRunningUsingForkChannel();
    }

    public void shouldRequestHandlerRunningUsingForkChannelProgrammaticCreation() throws Exception {
        c=create(null, "A", true);
        fc = new ForkChannel(c, "lock", "lock-channel", new CENTRAL_LOCK2());
        _shouldRequestHandlerRunningUsingForkChannel();
    }

    public void shouldLockServiceGetLockNotUsingForkChannel() throws Exception {
        c = create(null, "A", false).connect(CLUSTER);
        LockService lockService = new LockService(c);
        Lock lock=lockService.getLock("myLock");
        try {
            boolean success=lock.tryLock(5, TimeUnit.SECONDS);
            assert success;
        }
        finally {
            lock.unlock();
        }
    }

    public void shouldRequestHandlerRunningNotUsingForkChannel() throws Exception{
        c = create(null, "A", false).connect(CLUSTER);
        CENTRAL_LOCK2 centralLock2 = c.getProtocolStack().findProtocol(CENTRAL_LOCK2.class);
        assert centralLock2.isCoord() && centralLock2.isRequestHandlerRunning();
    }


    protected void _shouldLockServiceGetLockUsingForkChannel() throws Exception {
        fc = new ForkChannel(c, "lock", "lock-channel", new CENTRAL_LOCK2());
        fc.connect("bla");
        LockService lockService = new LockService(fc);
        boolean isLocked = lockService.getLock("myLock").tryLock(5, TimeUnit.SECONDS);
        assert isLocked == true;
    }

    protected void _shouldRequestHandlerRunningUsingForkChannel() throws Exception{
        c.connect(CLUSTER);
        fc.connect("bla");
        CENTRAL_LOCK2 centralLock2 = fc.getProtocolStack().findProtocol(CENTRAL_LOCK2.class);
        assert centralLock2.isCoord() && centralLock2.isRequestHandlerRunning();
    }

    protected static JChannel create(String props, String name, boolean use_fork) throws Exception {
        if(props != null)
            return new JChannel(props).name(name);
        Protocol p=use_fork? new FORK() : new CENTRAL_LOCK2();
        return new JChannel(Util.getTestStack(p)).name(name);
    }
}
