package org.jgroups.protocols;

import org.jgroups.*;
import org.jgroups.util.MyReceiver;
import org.jgroups.util.Util;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

/**
 * @author Bela Ban
 * @since  5.0
 */
@Test(groups=Global.FUNCTIONAL,singleThreaded=true)
public class COMPRESS_Test {
    protected JChannel a, b;
    protected MyReceiver<Message> r1=new MyReceiver<Message>().rawMsgs(true), r2=new MyReceiver<Message>().rawMsgs(true);

    @BeforeMethod
    protected void setup() throws Exception {
        a=create("A").connect(COMPRESS_Test.class.getSimpleName());
        b=create("B").connect(COMPRESS_Test.class.getSimpleName());
        Util.waitUntilAllChannelsHaveSameView(10000, 500, a,b);
        a.setReceiver(r1); b.setReceiver(r2);
    }

    @AfterMethod
    protected void destroy() {Util.close(r2, r1, b, a);}


    public void testSimpleCompressionBytesMessage() throws Exception {
        byte[] array=Util.generateArray(100);
        Message msg=new BytesMessage(b.getAddress(), array);
        _testSimpleCompression(msg);
    }

    public void testSimpleCompressionObjectMessage() throws Exception {
        byte[] array=Util.generateArray(100);
        Message msg=new ObjectMessage(b.getAddress(), array);
        _testSimpleCompression(msg);
    }


    public void _testSimpleCompression(Message m) throws Exception {
        byte[] array=Util.generateArray(100);
        a.send(m);
        Util.waitUntil(10000, 500, () -> r2.size() > 0);
        Message msg=r2.list().get(0);
        assert msg.hasPayload();
        byte[] array2=msg.getObject();
        assert array2.length == array.length;
        Util.verifyArray(array2);
    }


    protected static JChannel create(String name) throws Exception {
        return new JChannel(Util.getTestStack(new COMPRESS().setMinSize(50))).name(name);
    }

}
