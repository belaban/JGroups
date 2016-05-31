/*
 * Created on 04-Jul-2004
 *
 * To change the template for this generated file go to
 * Window - Preferences - Java - Code Generation - Code and Comments
 */
package org.jgroups.protocols;


import org.jgroups.*;
import org.jgroups.conf.ClassConfigurator;
import org.jgroups.stack.Protocol;
import org.jgroups.util.Util;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import javax.crypto.Cipher;
import java.io.*;
import java.security.MessageDigest;
import java.security.Security;
import java.util.*;

import static javax.swing.text.html.HTML.Tag.HEAD;

/**
 * @author xenephon
 */
@Test(groups=Global.FUNCTIONAL)
public class ENCRYPTAsymmetricTest {

    static final short ENCRYPT_ID=ClassConfigurator.getProtocolId(ENCRYPT.class);

    @BeforeClass
    static void initProvider() {
        Security.addProvider(new org.bouncycastle.jce.provider.BouncyCastleProvider());
    }

    public static void testInitNoProperties() throws Exception {

        ENCRYPT encrypt=new ENCRYPT();
        encrypt.init();

        // test the default asymetric key
        assert "RSA".equals(encrypt.getAsymAlgorithm());
        assert encrypt.getAsymInit() == 512;
        assert "RSA".equals(encrypt.getKpair().getPublic().getAlgorithm());
        assert "X.509".equals(encrypt.getKpair().getPublic().getFormat());

        assert encrypt.getKpair().getPublic().getEncoded() != null;

        // test the default symetric key
        assert "AES".equals(encrypt.getSymAlgorithm());
        assert encrypt.getSymInit() == 128;
        assert "AES".equals(encrypt.getDesKey().getAlgorithm());
        assert "RAW".equals(encrypt.getDesKey().getFormat());
        assert encrypt.getDesKey().getEncoded() != null;

        //test the resulting ciphers
        System.out.println("Provider:" + encrypt.getAsymCipher().getProvider());
        assert encrypt.getAsymCipher() != null;
        assert encrypt.getSymDecodingCipher() != null;
        assert encrypt.getSymEncodingCipher() != null;
    }

    public static void testInitBCAsymProperties() throws Exception {
        
        ENCRYPT encrypt=new ENCRYPT();
        encrypt.asymAlgorithm = "RSA";
        // encrypt.asymProvider = "BC";
        encrypt.init();

        // test the default asymetric key
        assert "RSA".equals(encrypt.getAsymAlgorithm());
        assert encrypt.getAsymInit() == 512;
        assert "RSA".equals(encrypt.getKpair().getPublic().getAlgorithm());
        //Strangely this returns differently from the default provider for RSA which is also BC!
        assert "X.509".equals(encrypt.getKpair().getPublic().getFormat());
        assert encrypt.getKpair().getPublic().getEncoded() != null;

        //test the resulting ciphers
        assert encrypt.getAsymCipher() != null;

    }



    @Test(expectedExceptions=Exception.class)
    public static void testInitIDEAProperties() throws Exception {    
        ENCRYPT encrypt=new ENCRYPT();
        encrypt.symAlgorithm =  "IDEA";
        encrypt.symInit = 128;
        encrypt.init();        
    }


    public static void testInitAESProperties() throws Exception {       
        ENCRYPT encrypt=new ENCRYPT();
        encrypt.symAlgorithm = "AES";
        encrypt.symInit = 128;     
        encrypt.init();

        // test the default symetric key
        assert "AES".equals(encrypt.getSymAlgorithm()) : "expected AES but was " + encrypt.getSymAlgorithm();
        Util.assertEquals(128, encrypt.getSymInit());
        Util.assertEquals("AES", encrypt.getDesKey().getAlgorithm());
        Util.assertEquals("RAW", encrypt.getDesKey().getFormat());
        Util.assertNotNull(encrypt.getDesKey().getEncoded());

        //test the resulting ciphers

        Util.assertNotNull(encrypt.getSymDecodingCipher());
        Util.assertNotNull(encrypt.getSymEncodingCipher());
    }

    public static void testViewChangeBecomeKeyserver() throws Exception {
        // set up the peer
        ENCRYPT encrypt=new ENCRYPT();
        encrypt.init();

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

        // String symVersion=new String(digest.digest(), "UTF-8");
        String symVersion=ENCRYPT.byteArrayToHexString(digest.digest());

        encrypt.keyServer=false;
        Message msg=new Message();
        msg.setBuffer(cipher.doFinal("hello".getBytes()));
        msg.putHeader(ENCRYPT_ID, new ENCRYPT.EncryptHeader(ENCRYPT.EncryptHeader.ENCRYPT, symVersion));

        Event evt=new Event(Event.MSG, msg);

        //pass in event to encrypt layer

        encrypt.up(evt);

        // assert that message is queued as we have no key
        Util.assertTrue(observer.getUpMessages().isEmpty());

        // send a view change to trigger the become key server
        // we use the fact that our address is now the controller one
        List<Address> tempVector=new ArrayList<Address>() ;
        tempVector.add(tempAddress);
        View tempView=new View(new ViewId(tempAddress, 1), tempVector);
        Event event=new Event(Event.VIEW_CHANGE, tempView);
        // this should have changed us to the key server
        encrypt.up(event);

        // send another encrypted message
        Message msg2=new Message();
        msg2.setBuffer(cipher.doFinal("hello2".getBytes()));
        msg2.putHeader(ENCRYPT_ID, new ENCRYPT.EncryptHeader(ENCRYPT.EncryptHeader.ENCRYPT, symVersion));

        // we should have three messages now in our observer
        // that are decrypted

        Event evt2=new Event(Event.MSG, msg2);

        encrypt.up(evt2);
        Util.assertEquals(3, observer.getUpMessages().size());


        Event sent=(Event)observer.getUpMessages().get("message1");


        Util.assertEquals("hello", new String(((Message)sent.getArg()).getBuffer()));

        sent=(Event)observer.getUpMessages().get("message2");

        Util.assertEquals("hello2", new String(((Message)sent.getArg()).getBuffer()));


    }


    public static void testViewChangeNewKeyServer() throws Exception {
        // create peer and server
        ENCRYPT peer=new ENCRYPT();
        peer.init();

        ENCRYPT server=new ENCRYPT();
        server.init();

        // set up server
        server.keyServer=true;
        MockObserver serverObserver=new MockObserver();
        server.setObserver(serverObserver);
        Address serverAddress=new MockAddress("server");

        server.setLocal_addr(serverAddress);
        //set the server up as keyserver
        List<Address> serverVector=new ArrayList<Address>();
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

        // String symVersion=new String(digest.digest(), "UTF-8");
        String symVersion=ENCRYPT.byteArrayToHexString(digest.digest());

        // encrypt and send an initial message to peer
        Cipher cipher=server.getSymEncodingCipher();
        Message msg=new Message();
        msg.setBuffer(cipher.doFinal("hello".getBytes()));
        msg.putHeader(ENCRYPT_ID, new ENCRYPT.EncryptHeader(ENCRYPT.EncryptHeader.ENCRYPT, symVersion));

        Event evt=new Event(Event.MSG, msg);

        peer.up(evt);
        //assert that message is queued as we have no key from server
        Util.assertTrue(peerObserver.getUpMessages().isEmpty());

        // send a view change where we are not the controller

        // send to peer - which should have peer2 as its key server
        peer.up(serverEvent);
        // assert that peer\ keyserver address is now set
        Util.assertEquals(serverAddress, peer.getKeyServerAddr());

        // get the resulting message from the peer - should be a key request

        Event sent=(Event)peerObserver.getDownMessages().get("message0");

        Util.assertEquals(((ENCRYPT.EncryptHeader)((Message)sent.getArg()).getHeader(ENCRYPT_ID)).getType(), ENCRYPT.EncryptHeader.KEY_REQUEST);
        Util.assertEquals(new String(((Message)sent.getArg()).getBuffer()), new String(peer.getKpair().getPublic().getEncoded()));

        // send this event to server
        server.up(sent);

        Event reply=(Event)serverObserver.getDownMessages().get("message1");

        //assert that reply is the session key encrypted with peer's public key
        Util.assertEquals(((ENCRYPT.EncryptHeader)((Message)reply.getArg()).getHeader(ENCRYPT_ID)).getType(), ENCRYPT.EncryptHeader.SECRETKEY);


        assert !peer.getDesKey().equals(server.getDesKey());
        // now send back to peer
        peer.up(reply);

        // assert that both now have same key
        Util.assertEquals(peer.getDesKey(), server.getDesKey());

        // send another encrypted message to peer to test queue
        Message msg2=new Message();
        msg2.setBuffer(cipher.doFinal("hello2".getBytes()));
        msg2.putHeader(ENCRYPT_ID, new ENCRYPT.EncryptHeader(ENCRYPT.EncryptHeader.ENCRYPT, symVersion));

        Event evt2=new Event(Event.MSG, msg2);

        peer.up(evt2);

        // make sure we have the events now in the up layers
        Util.assertEquals(3, peerObserver.getUpMessages().size());

        Event tempEvt=(Event)peerObserver.getUpMessages().get("message2");


        Util.assertEquals("hello", new String(((Message)tempEvt.getArg()).getBuffer()));

        tempEvt=(Event)peerObserver.getUpMessages().get("message3");

        Util.assertEquals("hello2", new String(((Message)tempEvt.getArg()).getBuffer()));


    }


    public static void testViewChangeNewKeyServerNewKey() throws Exception {
        // create peer and server
        ENCRYPT peer=new ENCRYPT();
        peer.init();

        ENCRYPT server=new ENCRYPT();
        server.init();

        ENCRYPT peer2=new ENCRYPT();
        peer2.init();

        // set up server
        server.keyServer=true;
        MockObserver serverObserver=new MockObserver();
        server.setObserver(serverObserver);

        //set the local address and view change to simulate a started instance
        Address serverAddress=new MockAddress("server");
        server.setLocal_addr(serverAddress);

        //	set the server up as keyserver
        List<Address> serverVector=new ArrayList<Address>() ;
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
        Util.assertTrue(peerObserver.getUpMessages().isEmpty());

        // send a view change to peer where peer2 is  controller
        List<Address> peerVector=new ArrayList<Address>() ;
        peerVector.add(peer2Address);
        View tempPeerView=new View(new ViewId(peer2Address, 1), peerVector);
        Event event=new Event(Event.VIEW_CHANGE, tempPeerView);

        // send to peer - should set peer2 as keyserver
        peer.up(event);

        // assert that peer\ keyserver address is now set
        Util.assertEquals(peer2Address, peer.getKeyServerAddr());

        // get the resulting message from the peer - should be a key request to peer2
        Event sent=(Event)peerObserver.getDownMessages().get("message0");

        // ensure type and that request contains peers pub key
        Util.assertEquals(((ENCRYPT.EncryptHeader)((Message)sent.getArg()).getHeader(ENCRYPT_ID)).getType(), ENCRYPT.EncryptHeader.KEY_REQUEST);
        Util.assertEquals(new String(((Message)sent.getArg()).getBuffer()), new String(peer.getKpair().getPublic().getEncoded()));

        //assume that server is no longer available and peer2 is new server
        // but did not get the key from server before assuming role
        // send this event to peer2
//		 send a view change to trigger the become key server
        // we use the fact that our address is now the controller one
        // send a view change where we are not the controller
        List<Address> peer2Vector=new ArrayList<Address>() ;
        peer2Vector.add(peer2Address);
        View tempPeer2View=new View(new ViewId(peer2Address, 1), peer2Vector);
        Event event2=new Event(Event.VIEW_CHANGE, tempPeer2View);
        // this should have changed us to the key server
        peer2.up(event2);

        peer2.up(sent);

        Event reply=(Event)peer2Observer.getDownMessages().get("message1");

        //assert that reply is the session key encrypted with peer's public key
        Util.assertEquals(((ENCRYPT.EncryptHeader)((Message)reply.getArg()).getHeader(ENCRYPT_ID)).getType(), ENCRYPT.EncryptHeader.SECRETKEY);


        assert !peer.getDesKey().equals(peer2.getDesKey());
        assert !server.getDesKey().equals(peer2.getDesKey());

        // now send back to peer
        peer.up(reply);

        // assert that both now have same key
        Util.assertEquals(peer.getDesKey(), peer2.getDesKey());
        assert !server.getDesKey().equals(peer.getDesKey());

        // send another encrypted message to peer to test queue
        Message msg2=new Message();
        msg2.setBuffer("hello2".getBytes());


        Event evt2=new Event(Event.MSG, msg2);

        peer2.down(evt2);

        Event Evt2=(Event)peer2Observer.getDownMessages().get("message2");

        peer.up(Evt2);
        // make sure we have the events now in the up layers
        Util.assertEquals(2, peerObserver.getUpMessages().size());

        Event tempEvt=(Event)peerObserver.getUpMessages().get("message2");


        Util.assertEquals("hello2", new String(((Message)tempEvt.getArg()).getBuffer()));


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
        }

        public void setProtocol(Protocol prot) {
        }

        public void passUp(Event evt) {
            storeUp(evt);
        }

        public void down(Event evt) {
        }

        public void passDown(Event evt) {
            storeDown(evt);
        }

        protected Map getUpMessages() {
            return upMessages;
        }

        protected void setUpMessages(Map upMessages) {
            this.upMessages=upMessages;
        }

        protected Map getDownMessages() {
            return downMessages;
        }

        protected void setDownMessages(Map downMessages) {
            this.downMessages=downMessages;
        }
    }

    static class MockAddress implements Address {
        String name;

        public MockAddress(String name) {
            this.name=name;
        }

        public MockAddress() {
        }

        public int size() {
            return 0;
        }

        public int compareTo(Address o) {
            return -1;
        }

        public boolean equals(Object obj) {
            MockAddress address=(MockAddress)obj;
            return address.name.equals(this.name);
        }

        public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        }

        public void writeExternal(ObjectOutput out) throws IOException {
        }

        public void writeTo(DataOutput out) throws Exception {
        }

        public void readFrom(DataInput in) throws Exception {
        }
    }


}
