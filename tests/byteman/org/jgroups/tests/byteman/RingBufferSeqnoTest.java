package org.jgroups.tests.byteman;

import org.jboss.byteman.contrib.bmunit.BMNGRunner;
import org.jboss.byteman.contrib.bmunit.BMScript;
import org.jgroups.Global;
import org.jgroups.util.RingBufferSeqnoLockless;
import org.testng.annotations.Test;

import java.util.List;

/**
 * Tests the correctness of RingBuffer
 * @author Bela Ban
 * @since 3.1
 */
@Test(groups={Global.BYTEMAN,Global.EAP_EXCLUDED},description="Correctness tests for RingBuffer",enabled=false)
public class RingBufferSeqnoTest extends BMNGRunner {



    

    /*@BMRules(rules=
               {
                 @BMRule(name="DelayRemove",
                         targetClass="org.jgroups.util.RingBuffer",
                         targetMethod="remove(boolean)",
                         targetLocation="WRITE hd",
                         action="System.out.println(\"---> Remover: waiting on \\\"remove\\\"\");\n" +
                           "   waitFor(\"remove\");\n" +
                           "   System.out.println(\"---> remover: was woken up\");"),
                 @BMRule(name="SignalAdd",
                         targetClass="org.jgroups.util.RingBuffer",
                         targetMethod="remove(boolean)",
                         targetLocation="EXIT",
                         action="System.out.println(\"---> Remover: signalling \\\"add\\\"\");\n" +
                           "   signalWake(\"add\", true);"),
                 @BMRule(name="DelayAdd",
                         targetClass="org.jgroups.util.RingBuffer",
                         targetMethod="add(long,java.lang.Object,boolean)",
                         targetLocation="READ low",
                         condition="!createCountDown(\"tmp\", 4) && countDown(\"tmp\")",
                         action="System.out.println(\"---> Adder: signaling \\\"remove\\\"\");\n" +
                           "   signalWake(\"remove\", true);\n" +
                           "   System.out.println(\"--> Adder: waiting for \\\"add\\\"\");\n" +
                           "   waitFor(\"add\");\n" +
                           "   System.out.println(\"--> Adder: awoken\");")
               })*/

    /**
     * Tests the following scenario:
     * HD=4, HR=5
     * T1 calls add(5). 5 already exists
     * T2 calls remove() with nullify=true
     * T1 reads HD, value is 4, continues (would terminate if HD was 5)
     * T2 checks that element at index HD+1 (5) is not null
     * T2 gets element at index HD+1 (5)
     * T2 increments HD to 5 and nulls the element at index 5
     * T1 does a CAS(null, 5) and succeeds because T2 just nulled the element
     ==> We now deliver the message at index 5 TWICE (or multiple times) !
     */
    @BMScript(dir="scripts/RingBufferSeqnoTest", value="testRemoveAndConcurrentAdd")
    public void testRemoveAndConcurrentAdd() throws InterruptedException {
        final RingBufferSeqnoLockless<Integer> buf=new RingBufferSeqnoLockless<>(10, 0);
        for(int i=1; i <= 5; i++)
            buf.add(i, i);
        buf.removeMany(true,4);
        System.out.println("buf = " + buf);

        Remover remover=new Remover(buf, 1);
        remover.start();

        Adder adder=new Adder(5, buf, false);
        adder.start();

        remover.join();
        adder.join();

        System.out.println("buf = " + buf);
        for(int i=1; i <=5; i++)
            System.out.println(i + ": " + buf._get(i));

        for(int i=1; i <=5; i++)
            assert buf._get(i) == null : "element " + i + " should be null, but isn't";
    }

    /**
     * Same as above, but using removeMany() rather than remove()
     * @throws InterruptedException
     */
    @BMScript(dir="scripts/RingBufferSeqnoTest", value="testRemoveAndConcurrentAdd2")
    public void testRemoveAndConcurrentAdd2() throws InterruptedException {
        final RingBufferSeqnoLockless<Integer> buf=new RingBufferSeqnoLockless<>(10, 0);
        for(int i=1; i <= 10; i++)
            buf.add(i, i);
        buf.removeMany(true, 4);
        System.out.println("buf = " + buf);

        Remover remover=new Remover(buf, 0);
        remover.start();

        Adder[] adders=new Adder[6];
        for(int i=0; i < adders.length; i++) {
            adders[i]=new Adder(i+5, buf, true);
            adders[i].start();
        }

        remover.join();
        for(Adder adder: adders)
            adder.join();

        System.out.println("buf = " + buf);
        for(int i=1; i <=10; i++)
            System.out.println(i + ": " + buf._get(i));

        for(int i=1; i <=10; i++)
            assert buf._get(i) == null : "element " + i + " should be null, but isn't";
    }



    protected static <T> void assertIndices(RingBufferSeqnoLockless<T> buf, long low, long hd, long hr) {
        assert buf.getLow() == low : "expected low=" + low + " but was " + buf.getLow();
        assert buf.getHighestDelivered() == hd : "expected hd=" + hd + " but was " + buf.getHighestDelivered();
        assert buf.getHighestReceived()  == hr : "expected hr=" + hr + " but was " + buf.getHighestReceived();
    }



    protected static class Adder extends Thread {
        protected final int                 seqno;
        protected final RingBufferSeqnoLockless<Integer> buf;
        protected final boolean             block;

        public Adder(int seqno, RingBufferSeqnoLockless<Integer> buf, boolean block) {
            this.seqno=seqno;
            this.buf=buf;
            this.block=block;
        }

        public void run() {
            boolean success=buf.add(seqno, seqno, block);
            // System.out.println("Adder: added " + seqno + (success? " successfully" : " unsuccessfully"));
        }
    }

    protected static class Remover extends Thread {
        protected final RingBufferSeqnoLockless<Integer> buf;
        protected final int remove_num_elements;

        public Remover(RingBufferSeqnoLockless<Integer> buf, int num) {
            this.buf=buf;
            remove_num_elements=num;
        }

        public void run() {
            if(remove_num_elements == 0) {
                List<Integer> list=buf.removeMany(true, remove_num_elements);
                System.out.println("Remover: removed " + list);
            }
            else {
                Integer element=buf.remove(true);
                System.out.println("Remover: removed " + element);
            }
        }
    }



}
