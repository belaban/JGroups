package org.jgroups.protocols;

import org.jgroups.Global;
import org.jgroups.JChannel;
import org.jgroups.protocols.pbcast.NAKACK2;
import org.jgroups.stack.ProtocolStack;
import org.jgroups.util.MyReceiver;
import org.jgroups.util.Util;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.stream.IntStream;
import java.util.stream.Stream;

/**
 * Tests {@link DAISYCHAIN}
 * @author Bela Ban
 * @since  5.0.0
 */
@Test(groups=Global.FUNCTIONAL)
public class DAISYCHAIN_Test {
    protected JChannel a, b, c;
    protected MyReceiver<Integer> r1=new MyReceiver<>(), r2=new MyReceiver<>(), r3=new MyReceiver<>();

    @BeforeMethod protected void setup() throws Exception {
        a=create("A").connect(getClass().getSimpleName());
        a.setReceiver(r1);
        b=create("B").connect(getClass().getSimpleName());
        b.setReceiver(r2);
        c=create("C").connect(getClass().getSimpleName());
        c.setReceiver(r3);
        Util.waitUntilAllChannelsHaveSameView(10000, 500, a,b,c);
    }

    @AfterMethod protected void destroy() {Util.close(c,b,a,r1,r2,r3);}


    public void testReception() throws Exception {
        for(int i=1; i <=30; i+=3) {
            a.send(null, i);
            b.send(null, i+1);
            c.send(null, i+2);
        }

        Util.waitUntil(10000, 500, () -> Stream.of(r1,r2,r3).allMatch(r -> r.list().size() == 30));
        Stream.of(r1,r2,r3).map(MyReceiver::list).peek(l -> System.out.printf("list: %s\n", l))
          .allMatch(l -> IntStream.rangeClosed(1,30).allMatch(l::contains));
    }


    protected static JChannel create(String name) throws Exception {
        JChannel ch=new JChannel(Util.getTestStack()).setName(name);
        DAISYCHAIN d=new DAISYCHAIN();
        ch.getProtocolStack().insertProtocol(d, ProtocolStack.Position.BELOW, NAKACK2.class);
        d.init();
        return ch;
    }
}
