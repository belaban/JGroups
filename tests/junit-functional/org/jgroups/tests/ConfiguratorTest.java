package org.jgroups.tests;

import org.jgroups.Global;
import org.jgroups.JChannel;
import org.jgroups.protocols.*;
import org.jgroups.protocols.pbcast.NAKACK2;
import org.jgroups.stack.Protocol;
import org.jgroups.stack.ProtocolStack;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.net.InetAddress;
import java.util.List;

/**
 * Tests ProtocolStack.insertProtocol() and removeProtocol()
 * @author Bela Ban
 */
@Test(groups=Global.FUNCTIONAL,singleThreaded=true)
public class ConfiguratorTest {
    protected JChannel      ch;
    protected ProtocolStack stack;
    static final String props="UDP:PING:FD_ALL:pbcast.NAKACK2(xmit_interval=500):UNICAST3:MFC";
    final String[] names={"MFC", "UNICAST3", "NAKACK2", "FD_ALL", "PING", "UDP"};
    final String[] below={"MFC", "UNICAST3", "TRACE", "NAKACK2", "FD_ALL", "PING", "UDP"};
    final String[] above={"MFC", "TRACE", "UNICAST3", "NAKACK2", "FD_ALL", "PING", "UDP"};



    @BeforeMethod
    void setUp() throws Exception {
        ch=new JChannel(new UDP(), new PING(), new FD_ALL(), new NAKACK2().setValue("xmit_interval", 500),
                        new UNICAST3(), new MFC());
        stack=ch.getProtocolStack();
    }

    
    public void testRemovalOfTop() throws Exception {
        Protocol prot=stack.removeProtocol("MFC");
        assert prot != null;
        List<Protocol> protocols=stack.getProtocols();
        Assert.assertEquals(5, protocols.size());
        assert protocols.get(0).getName().endsWith("UNICAST3");
        assert  stack.getTopProtocol().getUpProtocol() != null;
        assert  stack.getTopProtocol().getDownProtocol() != null;
        assert  stack.getTopProtocol().getDownProtocol().getUpProtocol() != null;
        assert  stack.getTopProtocol().getDownProtocol().getDownProtocol() != null;
    }
    
    public void testRemovalOfBottom() throws Exception {
        Protocol prot=stack.removeProtocol("UDP");
        assert prot != null;
        List<Protocol> protocols=stack.getProtocols();
        Assert.assertEquals(5, protocols.size());
        assert protocols.get(protocols.size() -1).getName().endsWith("PING");
    }
    
    public void testAddingAboveTop() throws Exception{
        Protocol new_prot=new TRACE();
        stack.insertProtocol(new_prot, ProtocolStack.Position.ABOVE, MFC.class);
        List<Protocol> protocols=stack.getProtocols();
        Assert.assertEquals(7, protocols.size());       
        assert protocols.get(0).getName().endsWith("TRACE");
        assert  stack.getTopProtocol().getUpProtocol() != null;
        assert  stack.getTopProtocol().getDownProtocol() != null;
        assert  stack.getTopProtocol().getDownProtocol().getUpProtocol() != null;
        assert  stack.getTopProtocol().getDownProtocol().getDownProtocol() != null;
    }
    
    @Test(expectedExceptions={IllegalArgumentException.class})
    public void testAddingBelowBottom() throws Exception{
        Protocol new_prot=new TRACE();
        stack.insertProtocol(new_prot, ProtocolStack.Position.BELOW, UDP.class);
    }
    
    

    public void testInsertion() throws Exception {
        List<Protocol> protocols=stack.getProtocols();
        assert protocols != null;
        Assert.assertEquals(6, protocols.size());

        for(int i=0; i < names.length; i++) {
            String name=names[i];
            Protocol p=protocols.get(i);
            Assert.assertEquals(name, p.getName());
        }

        // insert below
        Protocol new_prot=(Protocol)Class.forName("org.jgroups.protocols.TRACE").newInstance();
        stack.insertProtocol(new_prot, ProtocolStack.Position.BELOW, UNICAST3.class);
        protocols=stack.getProtocols();
        Assert.assertEquals(7, protocols.size());
        for(int i=0; i < below.length; i++) {
            String name=below[i];
            Protocol p=protocols.get(i);
            Assert.assertEquals(name, p.getName());
        }

        // remove
        Protocol prot=stack.removeProtocol("TRACE");
        assert prot != null;
        protocols=stack.getProtocols();
        Assert.assertEquals(6, protocols.size());
        for(int i=0; i < names.length; i++) {
            String name=names[i];
            Protocol p=protocols.get(i);
            Assert.assertEquals(name, p.getName());
        }

        // insert above
        new_prot=(Protocol)Class.forName("org.jgroups.protocols.TRACE").newInstance();
        stack.insertProtocol(new_prot, ProtocolStack.Position.ABOVE, UNICAST3.class);
        protocols=stack.getProtocols();
        Assert.assertEquals(7, protocols.size());
        for(int i=0; i < above.length; i++) {
            String name=above[i];
            Protocol p=protocols.get(i);
            Assert.assertEquals(name, p.getName());
        }
    }





    /** Tests that vars are substituted correctly when creating a channel programmatically (https://issues.jboss.org/browse/JGRP-1908) */
    public void testProgrammaticCreationAndVariableSubstitution() throws Exception {
        System.setProperty(Global.EXTERNAL_PORT, "10000");
        System.setProperty(Global.BIND_ADDR, "127.0.0.1");
        JChannel channel=new JChannel(
          new SHARED_LOOPBACK() /* dummy stack */
        ).name("A");

        TP tp=channel.getProtocolStack().getTransport();
        assert tp.getValue("external_port").equals(10000);
        assert tp.getValue("bind_addr").equals(InetAddress.getByName("127.0.0.1"));

    }


}
