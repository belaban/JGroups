/*
 * Created on 04-Jul-2004
 *
 * To change the template for this generated file go to
 * Window - Preferences - Java - Code Generation - Code and Comments
 */
package org.jgroups.protocols;

import junit.framework.TestCase;
import org.jgroups.*;
import org.jgroups.protocols.ENCRYPT.EncryptHeader;
import org.jgroups.stack.Protocol;

import javax.crypto.Cipher;
import java.io.*;
import java.security.MessageDigest;
import java.security.Security;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.Vector;

/**
 * @author xenephon
 * 
 * To change the template for this generated type comment go to Window -
 * Preferences - Java - Code Generation - Code and Comments
 */
public class ENCRYPTAsymmetricTest extends TestCase {

    {
        Security.addProvider(new org.bouncycastle.jce.provider.BouncyCastleProvider());

    }

    public void testInitNoProperties() {

        ENCRYPT encrypt=new ENCRYPT();
        try {
            encrypt.init();
        }
        catch(Exception e) {
            fail(e.getMessage());
        }

        // test the default asymetric key
        assertEquals("RSA", encrypt.getAsymAlgorithm());
        assertEquals(512, encrypt.getAsymInit());
        assertEquals("RSA", encrypt.getKpair().getPublic().getAlgorithm());
        assertEquals("X.509", encrypt.getKpair().getPublic().getFormat());

        assertNotNull(encrypt.getKpair().getPublic().getEncoded());

        // test the default symetric key
        assertEquals("Blowfish", encrypt.getSymAlgorithm());
        assertEquals(56, encrypt.getSymInit());
        assertEquals("Blowfish", encrypt.getDesKey().getAlgorithm());
        assertEquals("RAW", encrypt.getDesKey().getFormat());
        assertNotNull(encrypt.getDesKey().getEncoded());

        //test the resulting ciphers
        System.out.println("Provider:" + encrypt.getAsymCipher().getProvider());
        assertNotNull(encrypt.getAsymCipher());
        assertNotNull(encrypt.getSymDecodingCipher());
        assertNotNull(encrypt.getSymEncodingCipher());
    }

    public void testInitBCAsymProperties() {

        Properties props=new Properties();
        props.put("asym_provider", "BC");
        props.put("asym_algorithm", "RSA");
        //javax.
        ENCRYPT encrypt=new ENCRYPT();
        encrypt.setProperties(props);
        try {
            encrypt.init();
        }
        catch(Exception e) {
            fail(e.getMessage());
        }

        // test the default asymetric key
        assertEquals("RSA", encrypt.getAsymAlgorithm());
        assertEquals(512, encrypt.getAsymInit());
        assertEquals("RSA", encrypt.getKpair().getPublic().getAlgorithm());
        //Strangely this returns differently from the default provider for RSA which is also BC!
        assertEquals("X.509", encrypt.getKpair().getPublic().getFormat());
        assertNotNull(encrypt.getKpair().getPublic().getEncoded());

        //test the resulting ciphers
        assertNotNull(encrypt.getAsymCipher());

    }

    public void XtestInitRSABlockAsymProperties() {

        Properties props=new Properties();
        props.put("asym_algorithm", "RSA/ECB/OAEPPadding");
        //javax.
        ENCRYPT encrypt=new ENCRYPT();
        encrypt.setProperties(props);
        try {
            encrypt.init();
        }
        catch(Exception e) {
            fail(e.getMessage());
        }

        // test the default asymetric key
        assertEquals("RSA/ECB/OAEPPadding", encrypt.getAsymAlgorithm());
        assertEquals(512, encrypt.getAsymInit());
        assertEquals("RSA", encrypt.getKpair().getPublic().getAlgorithm());
        //Strangely this returns differently from the default provider for RSA which is also BC!
        assertEquals("X509", encrypt.getKpair().getPublic().getFormat());
        assertNotNull(encrypt.getKpair().getPublic().getEncoded());

        //test the resulting ciphers
        assertNotNull(encrypt.getAsymCipher());

    }

    public void testInitIDEAProperties() {
        Properties props=new Properties();
        props.put("sym_algorithm", "IDEA");
        props.put("sym_init", "128");

        ENCRYPT encrypt=new ENCRYPT();
        encrypt.setProperties(props);
        try {
            encrypt.init();
            fail("Should not have been able to initialize IDEA");
        }
        catch(Exception e) {

        }
    }

    public void testInitAESProperties() {
        Properties props=new Properties();
        props.put("sym_algorithm", "AES");
        props.put("sym_init", "128");
        ENCRYPT encrypt=new ENCRYPT();
        encrypt.setProperties(props);
        try {
            encrypt.init();
        }
        catch(Exception e) {
            fail(e.getMessage());
        }

        // test the default symetric key
        assertEquals("AES", encrypt.getSymAlgorithm());
        assertEquals(128, encrypt.getSymInit());
        assertEquals("AES", encrypt.getDesKey().getAlgorithm());
        assertEquals("RAW", encrypt.getDesKey().getFormat());
        assertNotNull(encrypt.getDesKey().getEncoded());

        //test the resulting ciphers

        assertNotNull(encrypt.getSymDecodingCipher());
        assertNotNull(encrypt.getSymEncodingCipher());
    }

    public void testViewChangeBecomeKeyserver() throws Exception {
        // set up the peer
        ENCRYPT encrypt=new ENCRYPT();
        try {
            encrypt.init();
        }
        catch(Exception e) {
            fail(e.getMessage());
        }

        // set in the observer
        MockAddress tempAddress=new MockAddress("encrypt");
        encrypt.setLocal_addr(tempAddress);
        MockObserver observer=new MockObserver();
        encrypt.setObserver(observer);

        // produce encrypted message
        Cipher cipher=encrypt.getSymEncodingCipher();

        MessageDigest digest=MessageDigest.getInstance("MD5");
        digest.reset();
        digest.update(encrypt.getDesKey().getEncoded());

        String symVersion=new String(digest.digest(), "UTF-8");

        encrypt.keyServer=false;
        Message msg=new Message();
        msg.setBuffer(cipher.doFinal("hello".getBytes()));
        msg.putHeader(EncryptHeader.KEY, new EncryptHeader(EncryptHeader.ENCRYPT, symVersion));

        Event evt=new Event(Event.MSG, msg);

        //pass in event to encrypt layer

        encrypt.up(evt);

        // assert that message is queued as we have no key
        assertTrue(observer.getUpMessages().isEmpty());

        // send a view change to trigger the become key server
        // we use the fact that our address is now the controller one
        Vector tempVector=new Vector();
        tempVector.add(tempAddress);
        View tempView=new View(new ViewId(tempAddress, 1), tempVector);
        Event event=new Event(Event.VIEW_CHANGE, tempView);
        // this should have changed us to the key server
        encrypt.up(event);

        // send another encrypted message
        Message msg2=new Message();
        msg2.setBuffer(cipher.doFinal("hello2".getBytes()));
        msg2.putHeader(EncryptHeader.KEY, new EncryptHeader(EncryptHeader.ENCRYPT, symVersion));

        // we should have three messages now in our observer
        // that are decrypted

        Event evt2=new Event(Event.MSG, msg2);

        encrypt.up(evt2);
        assertEquals(3, observer.getUpMessages().size());

        Event sent=(Event)observer.getUpMessages().get("message1");

        assertEquals("hello", new String(((Message)sent.getArg()).getBuffer()));

        sent=(Event)observer.getUpMessages().get("message2");

        assertEquals("hello2", new String(((Message)sent.getArg()).getBuffer()));

    }

    public void testViewChangeNewKeyServer() throws Exception {
        // create peer and server
        ENCRYPT peer=new ENCRYPT();
        try {
            peer.init();
        }
        catch(Exception e) {
            fail(e.getMessage());
        }

        ENCRYPT server=new ENCRYPT();
        try {
            server.init();
        }
        catch(Exception e) {
            fail(e.getMessage());
        }

        // set up server 
        server.keyServer=true;
        MockObserver serverObserver=new MockObserver();
        server.setObserver(serverObserver);
        Address serverAddress=new MockAddress("server");

        server.setLocal_addr(serverAddress);
        //set the server up as keyserver
        Vector serverVector=new Vector();
        serverVector.add(serverAddress);
        View tempView=new View(new ViewId(serverAddress, 1), serverVector);
        Event serverEvent=new Event(Event.VIEW_CHANGE, tempView);
        server.up(serverEvent);

        // set up peer
        Address peerAddress=new MockAddress("peer");
        peer.setLocal_addr(peerAddress);
        MockObserver peerObserver=new MockObserver();
        peer.setObserver(peerObserver);
        peer.keyServer=false;

        MessageDigest digest=MessageDigest.getInstance("MD5");
        digest.reset();
        digest.update(server.getDesKey().getEncoded());

        String symVersion=new String(digest.digest(), "UTF-8");

        // encrypt and send an initial message to peer
        Cipher cipher=server.getSymEncodingCipher();
        Message msg=new Message();
        msg.setBuffer(cipher.doFinal("hello".getBytes()));
        msg.putHeader(EncryptHeader.KEY, new EncryptHeader(EncryptHeader.ENCRYPT, symVersion));

        Event evt=new Event(Event.MSG, msg);

        peer.up(evt);
        //assert that message is queued as we have no key from server
        assertTrue(peerObserver.getUpMessages().isEmpty());

        // send a view change where we are not the controller

        // send to peer - which should have peer2 as its key server
        peer.up(serverEvent);
        // assert that peer\ keyserver address is now set
        assertEquals(serverAddress, peer.getKeyServerAddr());

        // get the resulting message from the peer - should be a key request

        Event sent=(Event)peerObserver.getDownMessages().get("message0");

        assertEquals(((EncryptHeader)((Message)sent.getArg()).getHeader(EncryptHeader.KEY)).getType(),
                     EncryptHeader.KEY_REQUEST);
        assertEquals(new String(((Message)sent.getArg()).getBuffer()),
                     new String(peer.getKpair().getPublic().getEncoded()));

        // send this event to server
        server.up(sent);

        Event reply=(Event)serverObserver.getDownMessages().get("message1");

        //assert that reply is the session key encrypted with peer's public key
        assertEquals(((EncryptHeader)((Message)reply.getArg()).getHeader(EncryptHeader.KEY)).getType(),
                     EncryptHeader.SECRETKEY);

        assertNotSame(peer.getDesKey(), server.getDesKey());
        // now send back to peer
        peer.up(reply);

        // assert that both now have same key
        assertEquals(peer.getDesKey(), server.getDesKey());

        // send another encrypted message to peer to test queue
        Message msg2=new Message();
        msg2.setBuffer(cipher.doFinal("hello2".getBytes()));
        msg2.putHeader(EncryptHeader.KEY, new EncryptHeader(EncryptHeader.ENCRYPT, symVersion));

        Event evt2=new Event(Event.MSG, msg2);

        peer.up(evt2);

        // make sure we have the events now in the up layers
        assertEquals(3, peerObserver.getUpMessages().size());

        Event tempEvt=(Event)peerObserver.getUpMessages().get("message2");

        assertEquals("hello", new String(((Message)tempEvt.getArg()).getBuffer()));

        tempEvt=(Event)peerObserver.getUpMessages().get("message3");

        assertEquals("hello2", new String(((Message)tempEvt.getArg()).getBuffer()));

    }

    public void testViewChangeNewKeyServerNewKey() throws Exception {
        // create peer and server
        ENCRYPT peer=new ENCRYPT();
        try {
            peer.init();
        }
        catch(Exception e) {
            fail(e.getMessage());
        }

        ENCRYPT server=new ENCRYPT();
        try {
            server.init();
        }
        catch(Exception e) {
            fail(e.getMessage());
        }

        ENCRYPT peer2=new ENCRYPT();
        try {
            peer2.init();
        }
        catch(Exception e) {
            fail(e.getMessage());
        }

        // set up server 
        server.keyServer=true;
        MockObserver serverObserver=new MockObserver();
        server.setObserver(serverObserver);

        //set the local address and view change to simulate a started instance
        Address serverAddress=new MockAddress("server");
        server.setLocal_addr(serverAddress);

        //	set the server up as keyserver
        Vector serverVector=new Vector();
        serverVector.add(serverAddress);
        View tempView=new View(new ViewId(serverAddress, 1), serverVector);
        Event serverEvent=new Event(Event.VIEW_CHANGE, tempView);
        server.up(serverEvent);

        // set up peer as if it has started but not recieved view change
        Address peerAddress=new MockAddress("peer");
        peer.setLocal_addr(peerAddress);
        MockObserver peerObserver=new MockObserver();
        peer.setObserver(peerObserver);
        peer.keyServer=false;

        // set up peer2 with server as key server
        Address peer2Address=new MockAddress("peer2");
        peer2.setLocal_addr(peer2Address);
        MockObserver peer2Observer=new MockObserver();
        peer2.setObserver(peer2Observer);
        peer2.keyServer=false;
        peer2.setKeyServerAddr(serverAddress);

        // send an encrypted message from the server
        Message msg=new Message();
        msg.setBuffer("hello".getBytes());

        Event evt=new Event(Event.MSG, msg);

        server.down(evt);

        // message0 is in response to view change
        Event encEvt=(Event)serverObserver.getDownMessages().get("message1");

        // sent to peer encrypted - should be queued in encyption layer as we do not have a keyserver set
        peer.up(encEvt);

        //assert that message is queued as we have no key from server
        assertTrue(peerObserver.getUpMessages().isEmpty());

        // send a view change to peer where peer2 is  controller
        Vector peerVector=new Vector();
        peerVector.add(peer2Address);
        View tempPeerView=new View(new ViewId(peer2Address, 1), peerVector);
        Event event=new Event(Event.VIEW_CHANGE, tempPeerView);

        // send to peer - should set peer2 as keyserver
        peer.up(event);

        // assert that peer\ keyserver address is now set
        assertEquals(peer2Address, peer.getKeyServerAddr());

        // get the resulting message from the peer - should be a key request to peer2		
        Event sent=(Event)peerObserver.getDownMessages().get("message0");

        // ensure type and that request contains peers pub key
        assertEquals(((EncryptHeader)((Message)sent.getArg()).getHeader(EncryptHeader.KEY)).getType(),
                     EncryptHeader.KEY_REQUEST);
        assertEquals(new String(((Message)sent.getArg()).getBuffer()),
                     new String(peer.getKpair().getPublic().getEncoded()));

        //assume that server is no longer available and peer2 is new server
        // but did not get the key from server before assuming role
        // send this event to peer2
        //		 send a view change to trigger the become key server
        // we use the fact that our address is now the controller one
        // send a view change where we are not the controller
        Vector peer2Vector=new Vector();
        peer2Vector.add(peer2Address);
        View tempPeer2View=new View(new ViewId(peer2Address, 1), peer2Vector);
        Event event2=new Event(Event.VIEW_CHANGE, tempPeer2View);
        // this should have changed us to the key server
        peer2.up(event2);

        peer2.up(sent);

        Event reply=(Event)peer2Observer.getDownMessages().get("message1");

        //assert that reply is the session key encrypted with peer's public key
        assertEquals(((EncryptHeader)((Message)reply.getArg()).getHeader(EncryptHeader.KEY)).getType(),
                     EncryptHeader.SECRETKEY);

        assertNotSame(peer.getDesKey(), peer2.getDesKey());
        assertNotSame(server.getDesKey(), peer2.getDesKey());

        // now send back to peer
        peer.up(reply);

        // assert that both now have same key
        assertEquals(peer.getDesKey(), peer2.getDesKey());
        assertNotSame(server.getDesKey(), peer.getDesKey());

        // send another encrypted message to peer to test queue
        Message msg2=new Message();
        msg2.setBuffer("hello2".getBytes());

        Event evt2=new Event(Event.MSG, msg2);

        peer2.down(evt2);

        Event Evt2=(Event)peer2Observer.getDownMessages().get("message2");

        peer.up(Evt2);
        // make sure we have the events now in the up layers
        assertEquals(2, peerObserver.getUpMessages().size());

        Event tempEvt=(Event)peerObserver.getUpMessages().get("message2");

        assertEquals("hello2", new String(((Message)tempEvt.getArg()).getBuffer()));

    }

    static class MockObserver implements ENCRYPT.Observer {

        private Map upMessages=new HashMap();
        private Map downMessages=new HashMap();
        private int counter=0;

        /* (non-Javadoc)
         * @see org.jgroups.UpHandler#up(org.jgroups.Event)
         */

        private void storeUp(Event evt) {
            upMessages.put("message" + counter++, evt);
        }

        private void storeDown(Event evt) {
            downMessages.put("message" + counter++, evt);
        }

        public void up(Event evt) {
            storeUp(evt);
            System.out.println("Up:" + evt.toString());
        }

        /* (non-Javadoc)
         * @see org.jgroups.stack.ProtocolObserver#setProtocol(org.jgroups.stack.Protocol)
         */
        public void setProtocol(Protocol prot) {}

        /* (non-Javadoc)
         * @see org.jgroups.stack.ProtocolObserver#up_prot.up(org.jgroups.Event)
         */
        public void passUp(Event evt) {
            storeUp(evt);
            System.out.println("PassUp:" + evt.toString());
        }

        /* (non-Javadoc)
         * @see org.jgroups.stack.ProtocolObserver#down(org.jgroups.Event, int)
         */
        public void down(Event evt) {
            System.out.println("down:" + evt.toString());
        }

        /* (non-Javadoc)
         * @see org.jgroups.stack.ProtocolObserver#down_prot.down(org.jgroups.Event)
         */
        public void passDown(Event evt) {
            storeDown(evt);
            System.out.println("passDown:" + evt.toString());
        }

        /**
         * @return Returns the upMessages.
         */
        protected Map getUpMessages() {
            return upMessages;
        }

        /**
         * @param upMessages
         *                The upMessages to set.
         */
        protected void setUpMessages(Map upMessages) {
            this.upMessages=upMessages;
        }

        /**
         * @return Returns the downMessages.
         */
        protected Map getDownMessages() {
            return downMessages;
        }

        /**
         * @param downMessages
         *                The downMessages to set.
         */
        protected void setDownMessages(Map downMessages) {
            this.downMessages=downMessages;
        }
    }

    class MockAddress implements Address {

        private static final long serialVersionUID=-479331506050129599L;

        /* (non-Javadoc)
         * @see org.jgroups.Address#isMulticastAddress()
         */
        String name;

        public MockAddress(String name) {
            this.name=name;
        }

        public MockAddress() {}

        public boolean isMulticastAddress() {
            return false;
        }

        public int size() {
            return 0;
        }

        /* (non-Javadoc)
         * @see java.io.Externalizable#readExternal(java.io.ObjectInput)
         */
        public void readExternal(ObjectInput in) throws IOException,ClassNotFoundException {}

        /* (non-Javadoc)
         * @see java.io.Externalizable#writeExternal(java.io.ObjectOutput)
         */
        public void writeExternal(ObjectOutput out) throws IOException {}

        /* (non-Javadoc)
         * @see java.lang.Comparable#compareTo(java.lang.Object)
         */
        public int compareTo(Object o) {
            return -1;
        }

        public boolean equals(Object obj) {
            MockAddress address=(MockAddress)obj;
            return address.name.equals(this.name);
        }

        public void writeTo(DataOutputStream out) throws IOException {}

        public void readFrom(DataInputStream in) throws IOException,
                                                IllegalAccessException,
                                                InstantiationException {}
    }

}
