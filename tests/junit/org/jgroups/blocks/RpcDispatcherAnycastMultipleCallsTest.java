package org.jgroups.blocks;

import org.jgroups.Global;
import org.jgroups.JChannel;
import org.jgroups.tests.ChannelTestBase;
import org.testng.annotations.AfterClass;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@Test(groups=Global.STACK_DEPENDENT, singleThreaded=true)
public class RpcDispatcherAnycastMultipleCallsTest extends ChannelTestBase {
    private RpcDispatcherAnycastServerObject[] targets=null;
    static final int NUM=3;

    @BeforeClass
    void init() throws Exception {
        targets=new RpcDispatcherAnycastServerObject[NUM];
        final String GROUP="RpcDispatcherAnycastMultipleCallsTest";
        JChannel first_channel=null;
        for(int i=0; i < NUM; i++) {
            JChannel c=createChannel();
            if(first_channel == null)
                first_channel=c;
            targets[i]=new RpcDispatcherAnycastServerObject(c);
        }

        List<JChannel> channels=Stream.of(targets).map(RpcDispatcherAnycastServerObject::getChannel).collect(Collectors.toList());
        makeUnique(channels);
        for(JChannel ch: channels)
            ch.connect(GROUP);
    }

    @AfterMethod
    protected void tearDown() throws Exception {
        Stream.of(targets).forEach(t -> t.i=0);
    }

    @AfterClass
    protected void shutdown() throws Exception {
        if(targets != null) {
            for(int i=0; i < targets.length; i++) {
                if(targets[i] != null) targets[i].shutdown();
                targets[i]=null;
            }
            targets=null;
        }
    }

    public void test2InstancesAnycastIncludeSelf() throws Exception {
        performTest(true, 2, false);
    }

    public void test3InstancesAnycastIncludeSelf() throws Exception {
        performTest(true, 3, false);
    }

    public void test2InstancesMcastIncludeSelf() throws Exception {
        performTest(false, 2, false);
    }

    public void test3InstancesMcastIncludeSelf() throws Exception {
        performTest(false, 3, false);
    }

    public void test2InstancesAnycastExcludeSelf() throws Exception {
        performTest(true, 2, true);
    }

    public void test3InstancesAnycastExcludeSelf() throws Exception {
        performTest(true, 3, true);
    }



    /**
     * Simple test that creates n instances of {@link org.jgroups.blocks.RpcDispatcherAnycastServerObject}, each one creates a Channel
     * and registers an RpcDispatcher.
     * <p/>
     * This test then calls {@link org.jgroups.blocks.RpcDispatcherAnycastServerObject#callRemote(boolean, boolean)} once on each of the n instances
     * and then tests that the method calls have in fact been executed on each of the n instances.
     * @param useAnycast  if true, anycast is used for remote calls.
     * @param n           number of instances
     * @param excludeSelf whether or not  to exclude self in rpc calls
     */
    private void performTest(boolean useAnycast, int n, boolean excludeSelf) throws Exception {

        // test that the target method has been invoked 0 times on each instance.
        for(int i=0; i < n; i++) assert 0 == targets[i].i;

        // if we don't exclude self, the state of all instances should be identical.
        int value=0;

        // if we are excluding self, we need an array of expected values.
        int[] expectedValues=null;

        if(excludeSelf) {
            expectedValues=new int[n];
            for(int i=0; i < n; i++) expectedValues[i]=0;
        }

        for(int instances=0; instances < n; instances++) {
            targets[instances].callRemote(useAnycast, excludeSelf);

            // the assertions and how we measure test success is different depending on whether we exclude self or not.

            if(excludeSelf) {
                for(int i=0; i < n; i++) {
                    if(i != instances) expectedValues[i]++;
                }
                for(int i=0; i < n; i++)
                    assert expectedValues[i] == targets[i].i;
            }
            else {
                value++;
                for(int i=0; i < n; i++)
                    assert value == targets[i].i;
            }
        }

    }
}
