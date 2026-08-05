package org.jgroups.protocols;

import org.jgroups.Global;
import org.jgroups.JChannel;
import org.jgroups.Message;
import org.jgroups.BytesMessage;
import org.jgroups.View;
import org.jgroups.protocols.pbcast.GMS;
import org.jgroups.protocols.pbcast.NAKACK2;
import org.jgroups.protocols.pbcast.STABLE;
import org.jgroups.stack.Protocol;
import org.jgroups.util.MessageBatch;
import org.jgroups.util.MyReceiver;
import org.jgroups.util.Util;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.jgroups.util.Util.waitUntilAllChannelsHaveSameView;

/**
 * Regression test for {@link ASYM_ENCRYPT} silently dropping a message encrypted with a previous group key after a
 * key rotation.
 *
 * <p><b>Real-world context</b> (does NOT model a permanent message loss): in the default stack layout {@code ASYM_ENCRYPT}
 * sits <em>below</em> {@code UNICAST3}/{@code NAKACK2}, so a message dropped at the decrypt layer is not seen by the
 * reliability protocols. They record the gap and ask the sender to retransmit, and the retransmitted copy is re-encrypted
 * with the sender's <em>current</em> key, so the application eventually receives it. Thus, in the well-behaved case, the
 * issue manifests as an unnecessary retransmission round-trip rather than a permanent loss.</p>
 *
 * <p>This makes the fix a robustness improvement: it avoids the drop (and hence the retransmission latency) and prevents
 * actual loss in the cases where retransmission cannot compensate, e.g. messages flagged {@link Message.Flag#NO_RELIABILITY},
 * members that do not converge on the new key, or stacks without UNICAST3/NAKACK2.</p>
 *
 * <p>The test drives the <em>real</em> protocol stack (a JChannel with ASYM_ENCRYPT, NAKACK2 and UNICAST3) so the
 * sub-suite takes retransmission into account: the still-in-flight message is delivered with the
 * {@link Message.Flag#NO_RELIABILITY} flag, which UNICAST3/NAKACK2 pass up without any sequencing or retransmission, so
 * no retransmission can rescue it. With the buggy code the message is dropped (key not cached); with the fix it is
 * delivered using the cached previous key.</p>
 *
 * <pre>
 * Scenario:
 *   A, B form a cluster with group key K1 (version V1).
 *   A encrypts an in-flight message with K1.
 *   The group key is rotated to K2 (version V2) on A and B (via a view change, the way coord changes / member churn
 *   cause it in production).
 *   The in-flight V1 message then arrives at B.
 *   Expectation: B delivers it (decrypting with the cached K1).
 *   Buggy code: B drops it, as key_map holds only the (V2 -&gt; K2) mapping.
 * </pre>
 *
 * @see Encrypt#decrypt
 */
@Test(groups=Global.ENCRYPT, singleThreaded=true)
public class EncryptOldKeyCacheTest extends EncryptTest {

    protected static final String PAYLOAD="hello world, encrypted with the old group key";

    @BeforeMethod protected void init() throws Exception {
        super.init();
    }

    @AfterMethod protected void destroy() {
        super.destroy();
    }

    public void testOldKeyMessageDeliveredAfterRotation() throws Exception {
        ASYM_ENCRYPT asym_a=a.getProtocolStack().findProtocol(ASYM_ENCRYPT.class);
        ASYM_ENCRYPT asym_b=b.getProtocolStack().findProtocol(ASYM_ENCRYPT.class);
        byte[] v1_a=asym_a.symVersion(), v1_b=asym_b.symVersion();
        assert v1_a != null && v1_b != null && Arrays.equals(v1_a, v1_b) :
          String.format("A and B should share the initial group key, but have V1a=%s V1b=%s",
                        Util.byteArrayToHexString(v1_a), Util.byteArrayToHexString(v1_b));
        System.out.printf("%s: initial symmetry version: %s\n", a.getAddress(),
                          Util.byteArrayToHexString(v1_a));

        // 1) an in-flight message encrypted with the current key K1 (version V1), as if sent just before the rotation
        Message in_flight=asym_a.encrypt(new BytesMessage(null).setArray(PAYLOAD.getBytes()));
        // NO_RELIABILITY: UNICAST3/NAKACK2 will pass it up without sequencing/retransmission, so no retransmission can
        // rescue the message after the key rotation
        in_flight.setFlag(Message.Flag.NO_RELIABILITY);

        // 2) rotate the group key on A and B via a real view change (the way a member leave with change_key_on_leave
        //    causes it in production): C leaves, A (coord) generates a new key K2 and distributes it to B
        for(ASYM_ENCRYPT asym: Arrays.asList(asym_a, asym_b)) {
            asym.setChangeKeyOnLeave(true);
        }
        GMS gms_a=a.getProtocolStack().findProtocol(GMS.class);
        View view=View.create(a.getAddress(), a.getView().getViewId().getId()+1, a.getAddress(), b.getAddress());
        gms_a.castViewChangeAndSendJoinRsps(view, null, Collections.singletonList(b.getAddress()), null, null);
        waitUntilAllChannelsHaveSameView(10000, 500, a, b);

        byte[] v2_a=asym_a.symVersion(), v2_b=asym_b.symVersion();
        System.out.printf("%s: after rotation A=%s B=%s (keys cached: %s)\n", a.getAddress(),
                          Util.byteArrayToHexString(v2_a), Util.byteArrayToHexString(v2_b), asym_b.printCachedGroupKeys());
        assert !Arrays.equals(v1_a, v2_a) : "group key should have been rotated on A";
        assert Arrays.equals(v2_a, v2_b) : "A and B should converge on the new group key";

        // 3) the in-flight V1 message arrives at B *after* the rotation. Without the fix, key_map holds only the
        //    (V2 -> K2) mapping, so the V1 lookup fails and the message is dropped.
        b.setReceiver(rb=new MyReceiver<Message>().rawMsgs(true));
        Message tmp=in_flight.copy(true, true);
        // Deliver via the MessageBatch path: that is how messages arrive from the transport in production, and it is the
        // path that decrypts with the (possibly cached) key. Delivering a single Message to up() instead would go down the
        // cipher==null code() path in Encrypt._decrypt(), which always uses the current secret_key and never the cache.
        MessageBatch batch=new MessageBatch(Collections.singletonList(tmp));
        asym_b.up(batch);

        for(int i=0; i < 10 && rb.size() == 0; i++)
            Util.sleep(500);
        System.out.printf("%s: B received: %s\n", a.getAddress(), rb.list());
        assert rb.size() == 1 : String.format("A and B are in the same view, but B dropped the message encrypted " +
            "with the previous group key after the rotation (key_map holds: %s)", asym_b.printCachedGroupKeys());
        assert Arrays.equals(rb.list().get(0).getArray(), PAYLOAD.getBytes());
    }

    @Override protected JChannel create(String name, java.util.function.Consumer<List<Protocol>> c) throws Exception {
        List<Protocol> protocols=new ArrayList<>(Arrays.asList(
          new SHARED_LOOPBACK(),
          new SHARED_LOOPBACK_PING(),
          new ASYM_ENCRYPT().symKeylength(128).asymKeylength(512).asymAlgorithm("RSA"),
          new NAKACK2().useMcastXmit(false),
          new UNICAST3(),
          new STABLE(),
          new GMS().setJoinTimeout(2000)));
        if(c != null) {
            c.accept(protocols);
        }
        return new JChannel(protocols).name(name);
    }
}