package org.jgroups.tests;

import org.jgroups.*;
import org.jgroups.protocols.*;
import org.jgroups.util.MyReceiver;
import org.jgroups.util.Util;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.util.List;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

/**
 * Tests message sending and reception with different bundlers
 * @author Bela Ban
 * @since  5.4
 */
@Test(groups=Global.STACK_DEPENDENT,singleThreaded=true,dataProvider="createBundler")
public class BundlerTest extends ChannelTestBase {
    protected JChannel                   a,b;
    protected MyReceiver<Integer>        ra=new MyReceiver<>(), rb=new MyReceiver<>();
    protected static final int           NUM=5;
    protected static final List<Integer> EXPECTED=IntStream.rangeClosed(1,5).boxed().collect(Collectors.toList());

    protected void setup(Class<Bundler> cl) throws Exception {
        a=createChannel().name("A");
        b=createChannel().name("B");
        makeUnique(a,b);
        a.connect("BundlerTest");
        b.connect("BundlerTest");
        Util.waitUntilAllChannelsHaveSameView(5000, 50, a,b);
        a.setReceiver(ra.reset());
        b.setReceiver(rb.reset());
        for(JChannel ch: List.of(a, b)) {
            TP transport=ch.stack().getTransport();
            Bundler bundler=cl.getConstructor().newInstance();
            transport.setBundler(bundler);
        }
    }

    @AfterMethod protected void destroy() {
        Util.close(b,a);
    }

    @DataProvider
    public static Object[][] createBundler() {
        return new Object[][]{
          {TransferQueueBundler.class},
          {NoBundler.class},
          {PerDestinationBundler.class}
        };
    }

    public void testMulticast(Class<Bundler> cl) throws Exception {
        send(cl, () -> null, false, NUM, NUM);
    }

    public void testMulticastNoLoopback(Class<Bundler> cl) throws Exception {
        send(cl, () -> null, true, 0, NUM);
    }

    public void testUnicast(Class<Bundler> cl) throws Exception {
        send(cl, () -> b.address(), false, 0, NUM);
    }

    public void testUnicastNoLoopback(Class<Bundler> cl) throws Exception {
        send(cl, () -> b.address(), true, 0, NUM);
    }

    public void testUnicastToSelf(Class<Bundler> cl) throws Exception {
        send(cl, () -> a.address(), false, NUM, 0);
    }

    public void testUnicastToSelfNoLoopback(Class<Bundler> cl) throws Exception {
        send(cl, () -> a.address(), true, 0, 0);
    }

    public void testStop(Class<Bundler> cl) throws Exception {
        setup(cl);
        Util.sleep(500);
        System.out.print("-- closing channels");
    }

    protected void send(Class<Bundler> cl, Supplier<Address> dest, boolean dont_loopback, int expected_a, int expected_b)
      throws Exception {
        setup(cl);
        for(int i=1; i <= NUM; i++) {
            Message msg=new ObjectMessage(dest.get(), i);
            if(dont_loopback)
                msg.setFlag(Message.TransientFlag.DONT_LOOPBACK);
            a.send(msg);
        }
        Util.waitUntil(2000, 50, () -> ra.size() == expected_a && rb.size() == expected_b, () -> print(a,b));
        System.out.printf("%s\n", print(a,b));
        assert expected_a <= 0 || ra.list().equals(EXPECTED);
        assert expected_b <= 0 || rb.list().equals(EXPECTED);
    }

    protected static String print(JChannel... channels) {
        return Stream.of(channels).map(ch -> String.format("%s: %s", ch.address(), ((MyReceiver<Integer>)ch.getReceiver()).list()))
          .collect(Collectors.joining("\n"));

    }



}
