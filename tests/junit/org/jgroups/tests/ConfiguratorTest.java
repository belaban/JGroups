package org.jgroups.tests;

import junit.framework.TestCase;
import org.jgroups.stack.Protocol;
import org.jgroups.stack.ProtocolStack;
import org.jgroups.JChannel;

import java.util.List;

/**
 * Tests ProtocolStack.insertProtocol() and removeProtocol()
 * @author Bela Ban
 * @version $Id: ConfiguratorTest.java,v 1.3 2007/01/16 12:52:12 belaban Exp $
 */
public class ConfiguratorTest extends TestCase {
    ProtocolStack stack;
    static final String props="UDP(mcast_addr=225.1.2.3):PING:FD:pbcast.NAKACK:UNICAST:FC";
    final String[] names={"FC", "UNICAST", "NAKACK", "FD", "PING", "UDP"};
    final String[] below={"FC", "UNICAST", "TRACE", "NAKACK", "FD", "PING", "UDP"};
    final String[] above={"FC", "TRACE", "UNICAST", "NAKACK", "FD", "PING", "UDP"};

    public ConfiguratorTest(String name) {
        super(name);
    }

    public void setUp() throws Exception {
        super.setUp();

        JChannel mock_channel=new JChannel() {

        };

        stack=new ProtocolStack(mock_channel, props);
    }


    public void tearDown() throws Exception {
        super.tearDown();
    }


    public void testInsertion() throws Exception {
        stack.setup();
        List protocols=stack.getProtocols();
        assertNotNull(protocols);
        assertEquals(6, protocols.size());

        for(int i=0; i < names.length; i++) {
            String name=names[i];
            Protocol p=(Protocol)protocols.get(i);
            assertEquals(name, p.getName());
        }

        // insert below
        Protocol new_prot=(Protocol)Class.forName("org.jgroups.protocols.TRACE").newInstance();
        stack.insertProtocol(new_prot, ProtocolStack.BELOW, "UNICAST");
        protocols=stack.getProtocols();
        assertEquals(7, protocols.size());
        for(int i=0; i < below.length; i++) {
            String name=below[i];
            Protocol p=(Protocol)protocols.get(i);
            assertEquals(name, p.getName());
        }

        // remove
        Protocol prot=stack.removeProtocol("TRACE");
        assertNotNull(prot);
        protocols=stack.getProtocols();
        assertEquals(6, protocols.size());
        for(int i=0; i < names.length; i++) {
            String name=names[i];
            Protocol p=(Protocol)protocols.get(i);
            assertEquals(name, p.getName());
        }

        // insert above
        new_prot=(Protocol)Class.forName("org.jgroups.protocols.TRACE").newInstance();
        stack.insertProtocol(new_prot, ProtocolStack.ABOVE, "UNICAST");
        protocols=stack.getProtocols();
        assertEquals(7, protocols.size());
        for(int i=0; i < above.length; i++) {
            String name=above[i];
            Protocol p=(Protocol)protocols.get(i);
            assertEquals(name, p.getName());
        }
    }



    public static void main(String[] args) {
        String[] testCaseName={ConfiguratorTest.class.getName()};
        junit.textui.TestRunner.main(testCaseName);
    }

}
