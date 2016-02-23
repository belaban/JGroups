/*
 * Created on 04-Jul-2004
 *
 * To change the template for this generated file go to
 * Window - Preferences - Java - Code Generation - Code and Comments
 */
package org.jgroups.protocols;


import org.jgroups.*;
import org.jgroups.conf.ClassConfigurator;
import org.jgroups.protocols.ENCRYPT.EncryptHeader;
import org.jgroups.stack.Protocol;
import org.jgroups.util.MessageBatch;
import org.jgroups.util.Util;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import javax.crypto.Cipher;
import javax.crypto.SecretKey;
import java.security.MessageDigest;
import java.security.Security;
import java.util.TreeMap;

/**
 * @author xenephon
 * @author Bela Ban
 */
@Test(groups=Global.FUNCTIONAL, sequential=false)
public class ENCRYPTAsymmetricTest {

    protected static final short ENCRYPT_ID=ClassConfigurator.getProtocolId(ENCRYPT.class);
    protected static final Address encrypt_addr=Util.createRandomAddress("encrypt");
    protected static final Address server_addr=Util.createRandomAddress("server");
    protected static final Address peer_addr=Util.createRandomAddress("peer");
    protected static final Address peer2_addr=Util.createRandomAddress("peer2");


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
        encrypt.setLocalAddress(encrypt_addr);
        MockProtocol observer=new MockProtocol();
        encrypt.setUpProtocol(observer);

        // produce encrypted message
        Cipher cipher=encrypt.getSymEncodingCipher();

        MessageDigest digest=MessageDigest.getInstance("MD5");
        digest.reset();
        digest.update(encrypt.getDesKey().getEncoded());

        byte[] symVersion=digest.digest();
        encrypt.keyServer=false;
        Message msg=new Message().setBuffer(cipher.doFinal("hello".getBytes()))
          .putHeader(ENCRYPT_ID, new EncryptHeader(EncryptHeader.ENCRYPT, symVersion));

        encrypt.up(new Event(Event.MSG, msg));

        // assert that message is queued as we have no key
        Util.assertTrue(observer.upMessages.isEmpty());

        // send a view change to trigger the become key server
        // we use the fact that our address is now the controller one
        View tempView=View.create(encrypt_addr,1,encrypt_addr);
        Event event=new Event(Event.VIEW_CHANGE, tempView);
        // this should have changed us to the key server
        encrypt.up(event);

        // send another encrypted message
        Message msg2=new Message().setBuffer(cipher.doFinal("hello2".getBytes()))
          .putHeader(ENCRYPT_ID,new EncryptHeader(EncryptHeader.ENCRYPT,symVersion));

        // we should have three messages now in our observer that are decrypted
        encrypt.up(new Event(Event.MSG, msg2));
        Util.assertEquals(3, observer.upMessages.size());

        Event sent=observer.upMessages.get("message1");
        Util.assertEquals("hello", new String(((Message)sent.getArg()).getBuffer()));

        sent=observer.upMessages.get("message2");
        Util.assertEquals("hello2", new String(((Message)sent.getArg()).getBuffer()));
    }


    public static void testViewChangeNewKeyServer() throws Exception {
        ENCRYPT peer=new ENCRYPT();
        peer.init();

        ENCRYPT server=new ENCRYPT();
        server.init();

        // set up server
        server.keyServer=true;
        MockProtocol serverObserver=new MockProtocol();
        server.setUpProtocol(serverObserver);
        server.setDownProtocol(serverObserver);

        server.setLocalAddress(server_addr);
        Event viewChange = new Event(Event.VIEW_CHANGE, View.create(server_addr, 1, server_addr));
        server.up(viewChange);

        // set up peer
        peer.setLocalAddress(peer_addr);
        MockProtocol peerObserver=new MockProtocol();
        peer.setUpProtocol(peerObserver);
        peer.setDownProtocol(peerObserver);
        peer.keyServer=false;


        MessageDigest digest=MessageDigest.getInstance("MD5");
        digest.reset();
        digest.update(server.getDesKey().getEncoded());

        // encrypt and send an initial message to peer
        Cipher cipher=server.getSymEncodingCipher();
        byte[] symVersion=digest.digest();
        Message msg=new Message().setBuffer(cipher.doFinal("hello".getBytes()))
          .putHeader(ENCRYPT_ID, new EncryptHeader(EncryptHeader.ENCRYPT, symVersion));

        peer.up(new Event(Event.MSG, msg));
        //assert that message is queued as we have no key from server
        Util.assertTrue(peerObserver.upMessages.isEmpty());

        // send a view change where we are not the controller

        // send to peer - which should have peer2 as its key server
        peer.up(viewChange);
        // assert that peer\ keyserver address is now set
        Util.assertEquals(server_addr, peer.getKeyServerAddr());

        // get the resulting message from the peer - should be a key request

        Event sent=peerObserver.downMessages.get("message0");

        Util.assertEquals(((EncryptHeader)((Message)sent.getArg()).getHeader(ENCRYPT_ID)).getType(), EncryptHeader.KEY_REQUEST);
        Util.assertEquals(new String(((Message)sent.getArg()).getBuffer()), new String(peer.getKpair().getPublic().getEncoded()));

        // send this event to server
        server.up(sent);

        Event reply=serverObserver.downMessages.get("message1");

        //assert that reply is the session key encrypted with peer's public key
        Util.assertEquals(((EncryptHeader)((Message)reply.getArg()).getHeader(ENCRYPT_ID)).getType(), EncryptHeader.SECRETKEY);


        assert !peer.getDesKey().equals(server.getDesKey());
        // now send back to peer
        peer.up(reply);

        // assert that both now have same key
        Util.assertEquals(peer.getDesKey(), server.getDesKey());

        // send another encrypted message to peer to test queue
        Message msg2=new Message().setBuffer(cipher.doFinal("hello2".getBytes()))
          .putHeader(ENCRYPT_ID,new EncryptHeader(EncryptHeader.ENCRYPT,symVersion));
        peer.up(new Event(Event.MSG, msg2));

        // make sure we have the events now in the up layers
        Util.assertEquals(3,peerObserver.upMessages.size());

        Event tempEvt=peerObserver.upMessages.get("message2");
        Util.assertEquals("hello", new String(((Message)tempEvt.getArg()).getBuffer()));

        tempEvt=peerObserver.upMessages.get("message3");
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
        MockProtocol serverObserver=new MockProtocol();
        server.setUpProtocol(serverObserver);
        server.setDownProtocol(serverObserver);

        //set the local address and view change to simulate a started instance
        server.setLocalAddress(server_addr);

        Event serverEvent = new Event(Event.VIEW_CHANGE, View.create(server_addr, 1, server_addr));
        server.up(serverEvent);

        // set up peer as if it has started but not recieved view change
        peer.setLocalAddress(peer_addr);
        MockProtocol peerObserver=new MockProtocol();
        peer.setUpProtocol(peerObserver);
        peer.setDownProtocol(peerObserver);
        peer.keyServer=false;

        // set up peer2 with server as key server
        peer2.setLocalAddress(peer2_addr);
        MockProtocol peer2Observer=new MockProtocol();
        peer2.setUpProtocol(peer2Observer);
        peer2.setDownProtocol(peer2Observer);
        peer2.keyServer=false;
        peer2.setKeyServerAddr(server_addr);

        // send an encrypted message from the server
        Message msg=new Message().setBuffer("hello".getBytes());
        server.down(new Event(Event.MSG, msg));

        // message0 is in response to view change
        Event encEvt=serverObserver.downMessages.get("message1");

        // sent to peer encrypted - should be queued in encyption layer as we do not have a keyserver set
        peer.up(encEvt);

        //assert that message is queued as we have no key from server
        Util.assertTrue(peerObserver.upMessages.isEmpty());
        updateViewFor(peer, server, serverObserver, serverEvent, peerObserver);
        Util.assertFalse(peerObserver.upMessages.isEmpty());

        Event event = new Event(Event.VIEW_CHANGE, View.create(peer2_addr, 2, peer2_addr));

        // send to peer - should set peer2 as keyserver
        peer.up(event);

        // assert that peer\ keyserver address is now set
        Util.assertEquals(peer2_addr, peer.getKeyServerAddr());

        // get the resulting message from the peer - should be a key request to peer2
        Event sent=peerObserver.downMessages.get("message0");

        // ensure type and that request contains peers pub key
        Util.assertEquals(((EncryptHeader)((Message)sent.getArg()).getHeader(ENCRYPT_ID)).getType(), EncryptHeader.KEY_REQUEST);
        Util.assertEquals(new String(((Message)sent.getArg()).getBuffer()), new String(peer.getKpair().getPublic().getEncoded()));


        // this should have changed us to the key server
        peer2.up(event);

        peer2.up(sent);

        Event reply=peer2Observer.downMessages.get("message1");

        //assert that reply is the session key encrypted with peer's public key
        Util.assertEquals(((EncryptHeader)((Message)reply.getArg()).getHeader(ENCRYPT_ID)).getType(), EncryptHeader.SECRETKEY);


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

        Event evt3=peer2Observer.downMessages.get("message2");

        peer.up(evt3);
        // make sure we have the events now in the up layers
        Util.assertEquals(4, peerObserver.upMessages.size());

        Event tempEvt=peerObserver.getLatestUpMessage();
        Util.assertEquals("hello2", new String(((Message)tempEvt.getArg()).getBuffer()));
    }

    public void testKeyChangesDuringKeyServerChange() throws Exception {
        // create peers and server
        ENCRYPT peer=new ENCRYPT();
        peer.init();

        ENCRYPT server=new ENCRYPT();
        server.init();

        ENCRYPT peer2=new ENCRYPT();
        peer2.init();

        // set up server
        server.keyServer=true;
        MockProtocol serverObserver=new MockProtocol();
        server.setUpProtocol(serverObserver);
        server.setDownProtocol(serverObserver);

        //set the local address and view change to simulate a started instance
        server.setLocalAddress(server_addr);

        //	set the server up as keyserver
        Event serverEvent = new Event(Event.VIEW_CHANGE, View.create(server_addr, 1, server_addr));
        server.up(new Event(Event.TMP_VIEW, serverEvent.getArg()));
        server.up(serverEvent);

        peer.setLocalAddress(peer_addr);
        MockProtocol peerObserver=new MockProtocol();
        peer.setUpProtocol(peerObserver);
        peer.setDownProtocol(peerObserver);
        peer.keyServer=false;

        updateViewFor(peer, server, serverObserver, serverEvent, peerObserver);

        // set up peer2 with server as key server
        peer2.setLocalAddress(peer2_addr);
        MockProtocol peer2Observer=new MockProtocol();
        peer2.setUpProtocol(peer2Observer);
        peer2.setDownProtocol(peer2Observer);
        peer2.keyServer=false;
        updateViewFor(peer2, server, serverObserver, serverEvent, peer2Observer);

        Assert.assertEquals(server.getDesKey().getEncoded(), peer.getDesKey().getEncoded());
        Assert.assertEquals(server.getDesKey().getEncoded(), peer2.getDesKey().getEncoded());

        Event viewChange2 = new Event(Event.VIEW_CHANGE, View.create(peer2_addr, 2, peer2_addr));
        peer2.up(new Event(Event.TMP_VIEW, viewChange2.getArg()));
        peer2.up(viewChange2);

        updateViewFor(peer, peer2, peer2Observer, viewChange2, peerObserver);

        Assert.assertFalse(server.getDesKey().equals(peer.getDesKey()));
        Assert.assertEquals(peer.getDesKey().getEncoded(), peer2.getDesKey().getEncoded());

    }

    public static void testSymmetricKeyIsChangedOnViewChange() throws Exception{
        ENCRYPT server=new ENCRYPT();
        server.changeKeysOnViewChange=true;
        MockProtocol serverObserver=new MockProtocol();
        server.setDownProtocol(serverObserver);
        server.setUpProtocol(serverObserver);
        server.setLocalAddress(server_addr);
        server.init();

        //	set the server up as key server
        Event initalView = new Event(Event.VIEW_CHANGE, View.create(server_addr, 1, server_addr));
        server.up(new Event(Event.TMP_VIEW, initalView.getArg()));
        server.up(initalView);

        SecretKey key = server.getDesKey();

        //	Update the view with new member
        Event updatedView = new Event(Event.VIEW_CHANGE, View.create(server_addr, 2, peer_addr));
        server.up(new Event(Event.TMP_VIEW, updatedView.getArg()));
        server.up(updatedView);

        SecretKey keyAfterViewChange = server.getDesKey();
        Util.assertFalse(key.equals(keyAfterViewChange));
    }

	private static void updateViewFor(ENCRYPT peer, ENCRYPT keyServer, MockProtocol serverObserver, Event serverEvent,
                                      MockProtocol peerObserver) {
		peer.up(serverEvent);
        Event peerKeyRequest=peerObserver.getLatestDownMessage();
        keyServer.up(peerKeyRequest);
        Event serverKeyToPeer=serverObserver.getLatestDownMessage();
        peer.up(serverKeyToPeer);
	}



    static class MockProtocol extends Protocol {
        private final TreeMap<String, Event> upMessages=new TreeMap<>();
        private final TreeMap<String, Event> downMessages=new TreeMap<>();
        private int counter;


        public Object down(Event evt) {
            downMessages.put("message" + counter++, evt);
            return null;
        }

        public Object up(Event evt) {
            upMessages.put("message" + counter++, evt);
            return null;
        }

        public void up(MessageBatch batch) {
            throw new UnsupportedOperationException();
        }

        protected Event getLatestUpMessage() {
            return upMessages.isEmpty()? null : upMessages.lastEntry().getValue();
        }

        protected Event getLatestDownMessage() {
            return downMessages.isEmpty()? null : downMessages.lastEntry().getValue();
        }
    }



}
