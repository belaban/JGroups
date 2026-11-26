package org.jgroups.tests;

import org.jgroups.Address;
import org.jgroups.Global;
import org.jgroups.MergeView;
import org.jgroups.View;
import org.jgroups.ViewId;
import org.jgroups.protocols.pbcast.JoinRsp;
import org.jgroups.util.*;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.io.IOException;
import java.util.Arrays;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

/**
 * Tests that a {@link JoinRsp} can round-trip a {@link MergeView}.
 * <p/>
 * {@code JoinRsp.writeTo()} calls {@code view.writeTo()} polymorphically, so a MergeView also writes
 * its subgroups. {@code JoinRsp.readFrom()} however always creates a plain {@code new View()}, which
 * doesn't read the subgroups back. The leftover subgroup bytes desync the stream, and the following
 * {@code digest.readFrom(in, false)} reads the *number of subgroups* as the digest's member count.
 * Depending on how that count compares to the view size, iterating the digest (as
 * {@code NAKACK2.setDigest()} does) either blows up with an ArrayIndexOutOfBoundsException or
 * silently yields garbage seqnos.
 * <p/>
 * A coordinator sends a MergeView in a JOIN-RSP whenever it gets a JOIN-REQ from a member that is
 * already in its view (CoordGmsImpl.handleMembershipChange() -> GMS.getViewAndDigest()) while its
 * current view happens to be a MergeView.
 *
 * @author Claude
 */
@Test(groups=Global.FUNCTIONAL)
public class JoinRspMergeViewTest {
    protected Address a, b, c;

    @BeforeClass
    protected void setup() {
        a=Util.createRandomAddress("A");
        b=Util.createRandomAddress("B");
        c=Util.createRandomAddress("C");
    }

    /** Baseline: a regular view round-trips correctly */
    public void testRegularView() throws Exception {
        View view=View.create(a, 5, a, b, c);
        _testRoundTrip(view);
    }

    /** 3 members, 2 subgroups: digest ends up with 2 seqno pairs but 3 members -> AIOOBE on iteration */
    public void testMergeViewWithFewerSubgroupsThanMembers() throws Exception {
        View view=new MergeView(new ViewId(a, 5), Arrays.asList(a, b, c),
                                Arrays.asList(View.create(a, 1, a, b), View.create(c, 1, c)));
        _testRoundTrip(view);
    }

    /** 2 members, 2 subgroups: no exception, but the seqnos are silently wrong */
    public void testMergeViewWithSameSubgroupsAsMembers() throws Exception {
        View view=new MergeView(new ViewId(a, 5), Arrays.asList(a, b),
                                Arrays.asList(View.create(a, 1, a), View.create(b, 1, b)));
        _testRoundTrip(view);
    }

    /** 2 members, 3 subgroups: digest ends up with more seqno pairs than members */
    public void testMergeViewWithMoreSubgroupsThanMembers() throws Exception {
        View view=new MergeView(new ViewId(a, 5), Arrays.asList(a, b),
                                Arrays.asList(View.create(a, 1, a), View.create(b, 1, b),
                                              View.create(a, 2, a, b)));
        _testRoundTrip(view);
    }

    /**
     * A JOIN-RSP from an unpatched coordinator (which writes the MergeView's subgroups) must be rejected
     * with an IOException, so that GMS.readJoinRsp() logs it and the joiner retries the JOIN, rather than
     * an ArrayIndexOutOfBoundsException propagating out of JChannel.connect().
     */
    public void testJoinRspFromUnpatchedCoord() throws Exception {
        View view=new MergeView(new ViewId(a, 5), Arrays.asList(a, b, c),
                                Arrays.asList(View.create(a, 1, a, b), View.create(c, 1, c)));
        ByteArray buf=marshalLegacy(view, createDigest(view));
        try {
            JoinRsp rsp=Util.streamableFromBuffer(JoinRsp::new, buf.array(), buf.getOffset(), buf.getLength());
            for(Digest.Entry ignored: rsp.getDigest()) // what NAKACK2.setDigest() does
                ;
            throw new AssertionError("should have thrown an IOException, but got " + rsp);
        }
        catch(IOException ex) {
            System.out.printf("received expected exception: %s\n", ex);
        }
    }

    /** Marshals a JoinRsp the way 4.2.30 and earlier do: the view is written polymorphically, subgroups and all */
    protected static ByteArray marshalLegacy(View view, Digest digest) throws Exception {
        ByteArrayDataOutputStream out=new ByteArrayDataOutputStream(512);
        out.writeByte(1); // Util.writeStreamable(): non-null marker
        out.writeByte(1 | 2); // JoinRsp: VIEW_PRESENT | DIGEST_PRESENT
        view.writeTo(out);
        digest.writeTo(out, false);
        return out.getBuffer();
    }

    /**
     * Marshals a JoinRsp exactly as GMS.marshal(JoinRsp)/GMS.readJoinRsp() do, then asserts that the
     * digest survived the round trip and can be iterated (as NAKACK2.setDigest() does).
     */
    protected static void _testRoundTrip(View view) throws Exception {
        Digest digest=createDigest(view);
        assertEquals(digest.capacity(), view.size());

        ByteArray buf=Util.streamableToBuffer(new JoinRsp(view, digest));
        JoinRsp rsp=Util.streamableFromBuffer(JoinRsp::new, buf.array(), buf.getOffset(), buf.getLength());

        View rv=rsp.getView();
        assert view.equals(rv) : String.format("view changed: view=%s, deserialized view=%s", view, rv);

        Digest new_digest=rsp.getDigest();
        int count=0;
        for(Digest.Entry ignored: new_digest) // this is what NAKACK2.setDigest() does
            count++;
        assertEquals(count, view.size(), "digest has a different number of entries than the view has members");
        assert new_digest.equals(digest) :
          String.format("digest changed: expected %s, but got %s", digest, new_digest);
    }

    /** Creates a digest matching the view, as CoordGmsImpl does before sending a JOIN-RSP */
    protected static Digest createDigest(View view) {
        MutableDigest digest=new MutableDigest(view.getMembersRaw());
        long seqno=10;
        for(Address mbr: view.getMembersRaw())
            digest.set(mbr, seqno, seqno+=10);
        assertTrue(digest.allSet());
        return digest;
    }
}

