
package org.jgroups.tests;

import org.jgroups.Global;
import org.jgroups.JChannel;
import org.jgroups.blocks.MethodCall;
import org.jgroups.blocks.ReplicatedTree;
import org.jgroups.blocks.RequestOptions;
import org.jgroups.blocks.RpcDispatcher;
import org.jgroups.protocols.pbcast.GMS;
import org.jgroups.protocols.pbcast.STATE_TRANSFER;
import org.jgroups.util.Util;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.InvalidClassException;
import java.io.Serializable;
import java.lang.reflect.Method;
import java.util.concurrent.TimeUnit;

/**
 * Tests {@link org.jgroups.util.ObjectInputStreamWithClassloader}'s support for a configurable
 * {@code java.io.ObjectInputFilter} (JEP 290), used to protect RPC and state / replicated-map deserialization
 * against unsafe deserialization (gadget-chain) attacks. See {@link Global#DESERIALIZATION_FILTER}.
 * <p/>
 * The filter is read once, from a static initializer, when {@code ObjectInputStreamWithClassloader} is loaded
 * (same as every other {@code jgroups.*} system property) - it can't be toggled within an already-running JVM.
 * So, other than the default (unset) case, these tests spawn a fresh JVM with the property passed on the command
 * line, run one of the {@code main()} methods below in it, and check what it printed.
 * <p/>
 * {@link #testNoFilterConfigured()}/{@link #testFilterAllowsMatchingClass()}/{@link #testFilterRejectsClass()}
 * exercise {@code Util.objectTo/FromByteBuffer()} directly. {@link #testRpcRejectsGadgetClass()} and
 * {@link #testStateTransferRejectsGadgetClass()} go one level further and exercise the actual production code
 * paths this feature was built to protect: a real RPC call between two {@link JChannel}s (via
 * {@link RpcDispatcher}/{@link org.jgroups.blocks.RequestCorrelator}), and a real state transfer between two
 * {@link JChannel}s (via {@link ReplicatedTree}, one of the few building blocks whose {@code setState()} actually
 * routes through {@code Util}/{@code ObjectInputStreamWithClassloader} - see doc/design/DeserializationFilter.txt
 * for a building block that does NOT, {@code ReplicatedHashMap}, a known gap this fix doesn't yet close).
 * @author Bela Ban
 */
@Test(groups=Global.FUNCTIONAL)
public class ObjectInputFilterTest {

    /** A plain Serializable payload - the kind of object that goes through ObjectInputStream (not Streamable).
     *  Used as-is for the direct Util tests, and as the RPC argument / tree value for the RPC and state tests. */
    public static class Payload implements Serializable {
        private static final long serialVersionUID=1L;
        public final String value;
        public Payload(String value) {this.value=value;}
    }

    /** Default behavior (no filter configured): unaffected, objects round-trip as before. Runs in-process, since
     *  this is the state of the current JVM (the property isn't set by the test suite). */
    public void testNoFilterConfigured() throws Exception {
        Payload p=new Payload("hello");
        byte[] buf=Util.objectToByteBuffer(p);
        Payload p2=Util.objectFromByteBuffer(buf);
        Assert.assertEquals(p2.value, p.value);
    }

    /** An allow-list filter that permits this class: deserialization still succeeds */
    public void testFilterAllowsMatchingClass() throws Exception {
        String out=runWithFilter(Allow.class, "org.jgroups.tests.ObjectInputFilterTest$*;java.lang.*;!*");
        Assert.assertEquals(out, "OK:hello");
    }

    /** A filter that rejects this class: deserialization must fail with InvalidClassException (an IOException),
     *  rather than silently succeeding */
    public void testFilterRejectsClass() throws Exception {
        String out=runWithFilter(Reject.class, "!org.jgroups.tests.ObjectInputFilterTest$Payload;*");
        Assert.assertEquals(out, "REJECTED");
    }

    /** Serializes a Payload and deserializes it via Util (picking up whatever filter -D configured); prints
     *  "OK:&lt;value&gt;" on success. Run via {@link #runWithFilter} in a separate JVM, not directly by TestNG. */
    public static class Allow {
        public static void main(String[] args) throws Exception {
            Payload p=new Payload("hello");
            byte[] buf=Util.objectToByteBuffer(p);
            Payload p2=Util.objectFromByteBuffer(buf);
            System.out.print("OK:" + p2.value);
        }
    }

    /** Same, but expects deserialization to be rejected by the configured filter; prints "REJECTED" or
     *  "NOT_REJECTED". Run via {@link #runWithFilter} in a separate JVM, not directly by TestNG. */
    public static class Reject {
        public static void main(String[] args) throws Exception {
            Payload p=new Payload("hello");
            byte[] buf=Util.objectToByteBuffer(p);
            try {
                Util.objectFromByteBuffer(buf);
                System.out.print("NOT_REJECTED");
            }
            catch(InvalidClassException e) {
                System.out.print("REJECTED");
            }
        }
    }

    /** A filter that only denies an unrelated, made-up "gadget" class name lets a real two-channel RPC call
     *  (with a Payload argument) through unaffected - proves the filter doesn't break normal RPC traffic. */
    public void testRpcAllowsNormalTraffic() throws Exception {
        String out=runWithFilter(Rpc.class, "!some.bogus.GadgetClass;*");
        assertContains(out, "OK:hello");
    }

    /** A filter that denies the Payload class rejects it as an RPC argument between two real channels - i.e. the
     *  filter genuinely protects RequestCorrelator/MethodCall unmarshalling, not just the raw Util helper. */
    public void testRpcRejectsGadgetClass() throws Exception {
        String out=runWithFilter(Rpc.class, "!org.jgroups.tests.ObjectInputFilterTest$Payload;*", "reject");
        assertNotContains(out, "NOT_REJECTED");
        assertContains(out, "REJECTED");
    }

    /** Same idea, but for state transfer: a filter denying only an unrelated class doesn't break a real state
     *  transfer between two channels via {@link ReplicatedTree}. */
    public void testStateTransferAllowsNormalTraffic() throws Exception {
        String out=runWithFilter(State.class, "!some.bogus.GadgetClass;*");
        assertContains(out, "OK:hello");
    }

    /** A filter denying the Payload class causes the joiner's state transfer to fail - i.e. the filter genuinely
     *  protects real getState()/setState() deserialization (ReplicatedTree.setState() -> Util.objectFromStream()),
     *  not just the raw Util helper. */
    public void testStateTransferRejectsGadgetClass() throws Exception {
        String out=runWithFilter(State.class, "!org.jgroups.tests.ObjectInputFilterTest$Payload;*", "reject");
        assertNotContains(out, "NOT_REJECTED");
        assertContains(out, "REJECTED");
    }

    /** The RPC/state harnesses below print their result marker last, right before exiting, but stdout may also
     *  contain incidental JGroups startup noise ahead of it (e.g. SHARED_LOOPBACK_PING probing real multicast on
     *  every network interface the machine has - VPN tunnels, Apple Wireless Direct Link, etc. - each logging a
     *  harmless "Can't assign requested address" warning). That noise is unrelated to whether the filter worked,
     *  so check the tail of the output instead of an exact match. */
    protected static void assertEndsWith(String output, String expected_suffix) {
        Assert.assertTrue(output.endsWith(expected_suffix),
                           "expected output to end with \"" + expected_suffix + "\" but was: " + output);
    }

    protected static void assertContains(String output, String expected_suffix) {
        Assert.assertTrue(output.contains(expected_suffix),
                          "expected output to contain \"" + expected_suffix + "\" but was: " + output);
    }

    protected static void assertNotContains(String output, String expected_suffix) {
        Assert.assertFalse(output.contains(expected_suffix),
                           "expected output not to contain \"" + expected_suffix + "\" but was: " + output);
    }

    /**
     * Connects two {@link JChannel}s (in-memory {@code SHARED_LOOPBACK} transport, no real sockets) and makes a
     * real, synchronous RPC call from one to the other with a {@link Payload} argument, so the argument is
     * unmarshalled via the real production path: incoming message -&gt; {@link org.jgroups.blocks.RequestCorrelator}
     * -&gt; {@link MethodCall#readFrom} -&gt; {@code Util.objectFromStream()} -&gt;
     * {@code ObjectInputStreamWithClassloader}. Uses {@code MethodCall}'s ID+{@code MethodLookup} mode so the only
     * class actually needing java-serialization is {@link Payload} itself (no {@code Method}/{@code Class}
     * metadata gets serialized along with it), keeping the filter patterns used above simple.
     * <p/>
     * Prints "OK:&lt;value&gt;" on success, or "REJECTED"/"NOT_REJECTED"/"UNEXPECTED_EXCEPTION:..." depending on
     * args[0]. Run via {@link #runWithFilter} in a separate JVM, not directly by TestNG.
     */
    public static class Rpc {
        protected static final short ECHO_ID=1;

        public Payload echo(Payload p) {return p;}

        public static void main(String[] args) throws Exception {
            boolean expect_reject=args.length > 0 && "reject".equals(args[0]);
            Method echo=Rpc.class.getMethod("echo", Payload.class);
            JChannel a=createChannel("A"), b=createChannel("B");
            Rpc target=new Rpc();
            RpcDispatcher disp1=new RpcDispatcher(a, target).setMethodLookup(id -> echo);
            RpcDispatcher disp2=new RpcDispatcher(b, target).setMethodLookup(id -> echo);
            try {
                a.connect("ObjectInputFilterTest-RPC");
                b.connect("ObjectInputFilterTest-RPC");
                Util.waitUntilAllChannelsHaveSameView(10000, 100, a, b);

                MethodCall call=new MethodCall(ECHO_ID, new Payload("hello"));
                Object result=disp1.callRemoteMethod(b.getAddress(), call, RequestOptions.SYNC().timeout(5000));
                System.out.print(expect_reject? ("NOT_REJECTED:" + result) : ("OK:" + ((Payload)result).value));
            }
            catch(Throwable t) {
                System.out.print(expect_reject? "REJECTED" : ("UNEXPECTED_EXCEPTION:" + t));
            }
            finally {
                Util.close(b, a);
            }
            System.exit(0); // don't wait on any lingering JGroups threads
        }

        protected static JChannel createChannel(String name) throws Exception {
            return new JChannel(testStack()).name(name);
        }
    }

    /**
     * Connects two {@link JChannel}s (in-memory transport plus a real {@link STATE_TRANSFER} protocol) wrapped in
     * {@link ReplicatedTree}, puts a {@link Payload} value into the first tree, then has the second channel join
     * and fetch the initial state from the first - so the value is unmarshalled via the real production path:
     * {@code channel.getState()} -&gt; {@link ReplicatedTree#setState} -&gt; {@code Util.objectFromStream()} -&gt;
     * {@code ObjectInputStreamWithClassloader}. A rejected filter surfaces as an
     * {@link org.jgroups.StateTransferException} wrapping the {@link InvalidClassException} (see
     * {@code JChannel.getState()}).
     * <p/>
     * Prints "OK:&lt;value&gt;" on success, or "REJECTED"/"NOT_REJECTED"/"UNEXPECTED_EXCEPTION:..." depending on
     * args[0]. Run via {@link #runWithFilter} in a separate JVM, not directly by TestNG.
     */
    public static class State {
        public static void main(String[] args) throws Exception {
            boolean expect_reject=args.length > 0 && "reject".equals(args[0]);
            JChannel a=createChannel("A"), b=createChannel("B");
            try {
                a.connect("ObjectInputFilterTest-State");
                ReplicatedTree tree_a=new ReplicatedTree(a); // first (only) member: getState() returns immediately
                tree_a.put("/data", "key1", new Payload("hello"));

                b.connect("ObjectInputFilterTest-State");
                ReplicatedTree tree_b=new ReplicatedTree(b); // joins & fetches real state from tree_a
                Object value=tree_b.get("/data", "key1");
                System.out.print(expect_reject? ("NOT_REJECTED:" + value) : ("OK:" + ((Payload)value).value));
            }
            catch(Throwable t) {
                System.out.print(expect_reject? "REJECTED" : ("UNEXPECTED_EXCEPTION:" + t));
            }
            finally {
                Util.close(b, a);
            }
            System.exit(0); // don't wait on any lingering JGroups threads
        }

        protected static JChannel createChannel(String name) throws Exception {
            return new JChannel(testStack(new STATE_TRANSFER())).name(name);
        }
    }

    /** {@link Util#getTestStack} defaults GMS's join_timeout to 1000ms, which is occasionally too tight on a
     *  slow/busy CI machine (a member that's briefly slow to respond to discovery gives up and becomes a
     *  singleton "cluster" instead of forming a single 2-member view). Bump it up for these forked-JVM tests,
     *  where an extra couple of seconds doesn't matter but flakiness does.
     *  <p/>
     *  Also disables the transport's diagnostics probe socket ({@code TP.enable_diagnostics}, on by default):
     *  it tries to join a multicast group on every network interface purely to answer external JMX-style probe
     *  queries - unrelated to actual message transport (SHARED_LOOPBACK's real traffic is in-memory) or to
     *  anything this test cares about - and on a machine with several VPN/utun interfaces, each failed multicast
     *  join logs a harmless but noisy warning to stdout, which these tests capture and would otherwise have to
     *  filter out. */
    protected static org.jgroups.stack.Protocol[] testStack(org.jgroups.stack.Protocol... additional) {
        org.jgroups.stack.Protocol[] stack=Util.getTestStack(additional);
        for(org.jgroups.stack.Protocol p: stack) {
            if(p instanceof org.jgroups.protocols.pbcast.GMS) {
                ((org.jgroups.protocols.pbcast.GMS)p).joinTimeout(5000);
                ((GMS)p).printLocalAddress(false);
            }
            if(p instanceof org.jgroups.protocols.TP)
                ((org.jgroups.protocols.TP)p).disableDiagnostics();
        }
        return stack;
    }

    /** Spawns "java -Djgroups.deserialization.filter=&lt;pattern&gt; -cp &lt;current classpath&gt; &lt;mainClass&gt;"
     *  (no extra program args) and returns what it printed to stdout. */
    protected static String runWithFilter(Class<?> mainClass, String pattern) throws IOException, InterruptedException {
        return runWithFilter(mainClass, pattern, (String[])null);
    }

    /** Same as {@link #runWithFilter(Class,String)}, but also passes {@code program_args} to the spawned
     *  {@code main()} (used to tell {@link Rpc}/{@link State} whether "reject" is expected). Output from stdout
     *  and stderr is merged, to make failure diagnosis easier when something other than the expected marker
     *  string comes back. */
    protected static String runWithFilter(Class<?> mainClass, String pattern, String... program_args)
      throws IOException, InterruptedException {
        String java_bin=System.getProperty("java.home") + File.separator + "bin" + File.separator + "java";
        String classpath=System.getProperty("java.class.path");
        java.util.List<String> cmd=new java.util.ArrayList<>(java.util.Arrays.asList(
          java_bin, "-cp", classpath, "-D" + Global.DESERIALIZATION_FILTER + "=" + pattern));
        cmd.add("-Djgroups.use.noop_logger=true"); // suppresses log output
        cmd.add(mainClass.getName());
        if(program_args != null)
            cmd.addAll(java.util.Arrays.asList(program_args));

        ProcessBuilder pb=new ProcessBuilder(cmd);
        pb.redirectErrorStream(true);
        Process proc=pb.start();
        String output;
        try(BufferedReader r=new BufferedReader(new InputStreamReader(proc.getInputStream()))) {
            StringBuilder sb=new StringBuilder();
            String line;
            while((line=r.readLine()) != null)
                sb.append(line);
            output=sb.toString();
        }
        boolean terminated=proc.waitFor(60, TimeUnit.SECONDS);
        Assert.assertTrue(terminated, "forked JVM (" + mainClass.getName() + ") did not terminate in time");
        return output;
    }
}
