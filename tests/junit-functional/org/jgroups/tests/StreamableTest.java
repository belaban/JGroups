
package org.jgroups.tests;


import org.jgroups.*;
import org.jgroups.protocols.*;
import org.jgroups.util.UUID;
import org.jgroups.util.Util;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.util.List;
import java.util.Vector;


@Test(groups=Global.FUNCTIONAL)
public class StreamableTest {

    static final short PING_ID=100;
    static final short UDP_ID=101;

    public static void testStreamable() throws Exception {
        byte[] buf={'b', 'e', 'l', 'a', 'b', 'a', 'n'};
        byte[] tmp;
        Message m1=new Message(null, null, buf, 0, 4);
        Message m2=new Message(null, null, buf, 4, 3);


        ByteArrayOutputStream output=new ByteArrayOutputStream();
        DataOutputStream out=new DataOutputStream(output);
        m1.writeTo(out);
        out.close();
        tmp=output.toByteArray();
        output.close();

        ByteArrayInputStream input=new ByteArrayInputStream(tmp);
        DataInputStream in=new DataInputStream(input);
        Message m3, m4;

        m3=new Message(false);
        m3.readFrom(in);

        Assert.assertEquals(4, m3.getLength());
        Assert.assertEquals(4, m3.getRawBuffer().length);
        Assert.assertEquals(4, m3.getBuffer().length);
        Assert.assertEquals(0, m3.getOffset());

        output=new ByteArrayOutputStream();
        out=new DataOutputStream(output);
        // out.writeObject(m2);
        m2.writeTo(out);
        out.close();
        tmp=output.toByteArray();
        output.close();

        System.out.println("-- serialized buffer is " + tmp.length + " bytes");

        input=new ByteArrayInputStream(tmp);
        in=new DataInputStream(input);

        m4=new Message();
        m4.readFrom(in);


        Assert.assertEquals(3, m4.getLength());
        Assert.assertEquals(3, m4.getBuffer().length);
        Assert.assertEquals(3, m4.getRawBuffer().length);
        Assert.assertEquals(0, m4.getOffset());
    }




    public static void testStreamable2() throws Exception {
        byte[] buf={'b', 'e', 'l', 'a', 'b', 'a', 'n'};
        Message msg=new Message(null, null, buf, 0, 4);
        stream(msg);
    }


    public static void testStreamable3() throws Exception {
        byte[] buf={'b', 'e', 'l', 'a', 'b', 'a', 'n'};
        Message msg=new Message(null, null, buf, 4, 3);
        stream(msg);
    }


    public static void testNullBuffer() throws Exception {
        Message msg=new Message();
        stream(msg);
    }



    public static void testNonNullBuffer() throws Exception {
        Message msg=new Message(null, null, "Hello world".getBytes());
        stream(msg);
    }



    public static void testNonNullAddress() throws Exception {
        stream(new Message(null, UUID.randomUUID(), "Hello world".getBytes()));
    }


    public static void testHeaders() throws Exception {
        Address dest=UUID.randomUUID();
        Address src=UUID.randomUUID();
        Message msg=new Message(dest, src, "Hello world".getBytes());
        PingHeader hdr=new PingHeader(PingHeader.GET_MBRS_REQ, new PingData(src, Util.createView(src, 1, src), true));
        msg.putHeader(PING_ID, hdr);
        TpHeader udp_hdr=new TpHeader("bla");
        msg.putHeader(UDP_ID, udp_hdr);
        stream(msg);
    }




    public static void testMergeView() throws Exception {
        Vector tmp_m1, tmp_m2 , m3, all, subgroups;
        Address a,b,c,d,e,f;
        View v1, v2, v3, v4, v5, view_all;

        a=UUID.randomUUID();
        b=UUID.randomUUID();
        c=UUID.randomUUID();
        d=UUID.randomUUID();
        e=UUID.randomUUID();
        f=UUID.randomUUID();

        tmp_m1=new Vector(); tmp_m2=new Vector(); m3=new Vector(); all=new Vector(); subgroups=new Vector();
        tmp_m1.add(a); tmp_m1.add(b); tmp_m1.add(c);
        tmp_m2.add(d);
        m3.add(e); m3.add(f);
        all.add(a); all.add(b); all.add(c); all.add(d); all.add(e); all.add(f);

        v1=new View(a, 1, tmp_m1);
        v2=new MergeView(d, 2, tmp_m2, new Vector());
        v3=new View(e, 3, m3);
        v4=new MergeView(e, 4, m3, null);
        v5=new View(e, 5, m3);
        subgroups.add(v1);
        subgroups.add(v2);
        subgroups.add(v3);
        subgroups.add(v4);
        subgroups.add(v5);

        view_all=new MergeView(a, 5, all, subgroups);
        System.out.println("MergeView: " + view_all);
        List<View> sub=((MergeView)view_all).getSubgroups();
        assert sub.get(0) instanceof View;
        assert sub.get(1) instanceof MergeView;
        assert sub.get(2) instanceof View;
        assert sub.get(3) instanceof MergeView;
        assert sub.get(4) instanceof View;

        byte[] buf=Util.streamableToByteBuffer(view_all);
        assert buf != null;
        assert buf.length > 0;

        MergeView merge_view=(MergeView)Util.streamableFromByteBuffer(MergeView.class, buf);
        assert merge_view != null;
        System.out.println("MergeView: " + merge_view);
        sub=merge_view.getSubgroups();
        assert sub.get(0) instanceof View;
        assert sub.get(1) instanceof MergeView;
        assert sub.get(2) instanceof View;
        assert sub.get(3) instanceof MergeView;
        assert sub.get(4) instanceof View;
    }

    private static void stream(Message msg) throws Exception {
        int length, bufLength;
        byte[] tmp;
        Message msg2;
        Address src, dest=msg.getDest();
        int num_headers=getNumHeaders(msg);

        length=msg.getLength();
        bufLength=getBufLength(msg);
        src=msg.getSrc();

        ByteArrayOutputStream output=new ByteArrayOutputStream();
        DataOutputStream out=new DataOutputStream(output);
        msg.writeTo(out);
        out.close();
        tmp=output.toByteArray();
        output.close();

        System.out.println("-- serialized buffer is " + tmp.length + " bytes");

        ByteArrayInputStream input=new ByteArrayInputStream(tmp);
        DataInputStream in=new DataInputStream(input);

        msg2=new Message();
        msg2.readFrom(in);

        Assert.assertEquals(length, msg2.getLength());
        Assert.assertEquals(bufLength, getBufLength(msg2));
        assert match(dest, msg2.getDest());
        assert match(src, msg2.getSrc());
        Assert.assertEquals(num_headers, getNumHeaders(msg2));
    }

    private static int getNumHeaders(Message msg) {
        return msg.getNumHeaders();
    }


    private static boolean match(Address a1, Address a2) {
        if(a1 == null && a2 == null)
            return true;
        if(a1 != null)
            return a1.equals(a2);
        else
            return a2.equals(a1);
    }

//    private int getRawBufLength(Message msg) {
//        return msg.getRawBuffer() != null? msg.getRawBuffer().length : 0;
//    }

    private static int getBufLength(Message msg) {
        return msg.getBuffer() != null? msg.getBuffer().length : 0;
    }


}
