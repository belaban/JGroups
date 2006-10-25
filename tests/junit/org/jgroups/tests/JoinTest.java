package org.jgroups.tests;

import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;
import org.jgroups.Channel;
import org.jgroups.ChannelException;
import org.jgroups.JChannel;
import org.jgroups.View;


/**
 * @author Bela Ban
 * @version $Id: JoinTest.java,v 1.1 2006/10/25 12:00:55 belaban Exp $
 */
public class JoinTest extends TestCase {
    Channel c1, c2;
    static String STACK="udp.xml";


    public JoinTest(String name) {
        super(name);
    }


    public void setUp() throws Exception {
        super.setUp();
        STACK=System.getProperty("stack", STACK);
        c1=new JChannel(STACK);
        c2=new JChannel(STACK);
    }


    protected void tearDown() throws Exception {
        super.tearDown();
        if(c1 != null)
            c1.close();
        if(c2 != null)
            c2.close();
    }

    public void testSingleJoin() throws ChannelException {
        c1.connect("X");
        View v=c1.getView();
        assertNotNull(v);
        assertEquals(1, v.size());
    }




    public static Test suite() {
        return new TestSuite(JoinTest.class);
    }

    public static void main(String[] args) {
        junit.textui.TestRunner.run(JoinTest.suite());
    }
}
