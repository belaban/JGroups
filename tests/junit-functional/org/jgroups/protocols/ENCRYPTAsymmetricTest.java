/*
 * Created on 04-Jul-2004
 *
 * To change the template for this generated file go to
 * Window - Preferences - Java - Code Generation - Code and Comments
 */
package org.jgroups.protocols;


import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.security.Security;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.TreeMap;

import javax.crypto.SecretKey;

import org.jgroups.Address;
import org.jgroups.Event;
import org.jgroups.Global;
import org.jgroups.Message;
import org.jgroups.View;
import org.jgroups.ViewId;
import org.jgroups.conf.ClassConfigurator;
import org.jgroups.protocols.ENCRYPT.EncryptHeader;
import org.jgroups.protocols.ENCRYPT.SymmetricCipherState;
import org.jgroups.stack.Protocol;
import org.jgroups.util.MessageBatch;
import org.jgroups.util.Util;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

/**
 * @author xenephon
 */
@Test(groups=Global.FUNCTIONAL, sequential=false)
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
		assert "AES".equals(encrypt.getSymState().getSecretKey().getAlgorithm());
		assert "RAW".equals(encrypt.getSymState().getSecretKey().getFormat());
		assert encrypt.getSymState().getSecretKey().getEncoded() != null;

		//test the resulting ciphers
		System.out.println("Provider:" + encrypt.getAsymCipher().getProvider());
		assert encrypt.getAsymCipher() != null;
		assert encrypt.getSymState() != null;
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
		Util.assertEquals("AES", encrypt.getSymState().getSecretKey().getAlgorithm());
		Util.assertEquals("RAW", encrypt.getSymState().getSecretKey().getFormat());
		Util.assertNotNull(encrypt.getSymState().getSecretKey().getEncoded());

		//test the resulting ciphers

		Util.assertNotNull(encrypt.getSymState());
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
		SymmetricCipherState state=encrypt.getSymState();


		encrypt.keyServer=false;
		Message msg=new Message();
		byte[] bytes = "hello".getBytes();
		msg.setBuffer(state.encryptMessage(bytes, 0	, bytes.length));
		msg.putHeader(ENCRYPT_ID, new EncryptHeader(EncryptHeader.ENCRYPT, state.getSymVersion()));

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
		byte[] bytes2 = "hello2".getBytes();
		msg2.setBuffer(state.encryptMessage(bytes2, 0	, bytes2.length));
		msg2.putHeader(ENCRYPT_ID, new EncryptHeader(EncryptHeader.ENCRYPT, state.getSymVersion()));

		// we should have three messages now in our observer
		// that are decrypted

		Event evt2=new Event(Event.MSG, msg2);

		encrypt.up(evt2);
		Util.assertEquals(3, observer.getUpMessages().size());


		Event sent=observer.getUpMessages().get("message1");


		Util.assertEquals("hello", new String(((Message)sent.getArg()).getBuffer()));

		sent=observer.getUpMessages().get("message2");

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
		Event serverEvent = createViewChange( 1, serverAddress);
		server.up(serverEvent);

		// set up peer
		Address peerAddress=new MockAddress("peer");
		peer.setLocal_addr(peerAddress);
		MockObserver peerObserver=new MockObserver();
		peer.setObserver(peerObserver);
		peer.keyServer=false;

		// encrypt and send an initial message to peer
		SymmetricCipherState state=server.getSymState();
		Message msg=new Message();
		byte[] bytes = "hello".getBytes();

		msg.setBuffer(state.encryptMessage(bytes, 0, bytes.length));
		msg.putHeader(ENCRYPT_ID, new EncryptHeader(EncryptHeader.ENCRYPT, state.getSymVersion()));

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

		Event sent=peerObserver.getDownMessages().get("message0");

		Util.assertEquals(((EncryptHeader)((Message)sent.getArg()).getHeader(ENCRYPT_ID)).getType(), EncryptHeader.KEY_REQUEST);
		Util.assertEquals(new String(((Message)sent.getArg()).getBuffer()), new String(peer.getKpair().getPublic().getEncoded()));

		// send this event to server
		server.up(sent);

		Event reply=serverObserver.getDownMessages().get("message1");

		//assert that reply is the session key encrypted with peer's public key
		Util.assertEquals(((EncryptHeader)((Message)reply.getArg()).getHeader(ENCRYPT_ID)).getType(), EncryptHeader.SECRETKEY);


		assert !peer.getSymState().getSecretKey().equals(server.getSymState().getSecretKey());
		// now send back to peer
		peer.up(reply);

		// assert that both now have same key
		Util.assertEquals(peer.getSymState().getSecretKey(), server.getSymState().getSecretKey());

		// send another encrypted message to peer to test queue
		Message msg2=new Message();
		byte[] bytes2 = "hello2".getBytes();

		msg2.setBuffer(state.encryptMessage(bytes2, 0, bytes2.length));
		msg2.putHeader(ENCRYPT_ID, new EncryptHeader(EncryptHeader.ENCRYPT, state.getSymVersion()));

		Event evt2=new Event(Event.MSG, msg2);

		peer.up(evt2);

		// make sure we have the events now in the up layers
		Util.assertEquals(3, peerObserver.getUpMessages().size());

		Event tempEvt=peerObserver.getUpMessages().get("message2");


		Util.assertEquals("hello", new String(((Message)tempEvt.getArg()).getBuffer()));

		tempEvt=peerObserver.getUpMessages().get("message3");

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

		Event serverEvent = createViewChange(1, serverAddress);
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
		Event encEvt=serverObserver.getDownMessages().get("message1");

		// sent to peer encrypted - should be queued in encyption layer as we do not have a keyserver set
		peer.up(encEvt);

		//assert that message is queued as we have no key from server
		Util.assertTrue(peerObserver.getUpMessages().isEmpty());
		updateViewFor(peer, server, serverObserver, serverEvent, peerObserver);
		Util.assertFalse(peerObserver.getUpMessages().isEmpty());

		Event event = createViewChange(2, peer2Address);

		// send to peer - should set peer2 as keyserver
		peer.up(event);

		// assert that peer\ keyserver address is now set
		Util.assertEquals(peer2Address, peer.getKeyServerAddr());

		// get the resulting message from the peer - should be a key request to peer2
		Event sent=peerObserver.getDownMessages().get("message0");

		// ensure type and that request contains peers pub key
		Util.assertEquals(((EncryptHeader)((Message)sent.getArg()).getHeader(ENCRYPT_ID)).getType(), EncryptHeader.KEY_REQUEST);
		Util.assertEquals(new String(((Message)sent.getArg()).getBuffer()), new String(peer.getKpair().getPublic().getEncoded()));


		// this should have changed us to the key server
		peer2.up(event);

		peer2.up(sent);

		Event reply=peer2Observer.getDownMessages().get("message1");

		//assert that reply is the session key encrypted with peer's public key
		Util.assertEquals(((EncryptHeader)((Message)reply.getArg()).getHeader(ENCRYPT_ID)).getType(), EncryptHeader.SECRETKEY);


		assert !peer.getSymState().getSecretKey().equals(peer2.getSymState().getSecretKey());
		assert !server.getSymState().getSecretKey().equals(peer2.getSymState().getSecretKey());

		// now send back to peer
		peer.up(reply);

		// assert that both now have same key
		Util.assertEquals(peer.getSymState().getSecretKey(), peer2.getSymState().getSecretKey());
		assert !server.getSymState().getSecretKey().equals(peer.getSymState().getSecretKey());

		// send another encrypted message to peer to test queue
		Message msg2=new Message();
		msg2.setBuffer("hello2".getBytes());


		Event evt2=new Event(Event.MSG, msg2);

		peer2.down(evt2);

		Event Evt2=peer2Observer.getDownMessages().get("message2");

		peer.up(Evt2);
		// make sure we have the events now in the up layers
		Util.assertEquals(4, peerObserver.getUpMessages().size());

		Event tempEvt=getLatestUpMessage(peerObserver);


		Util.assertEquals("hello2", new String(((Message)tempEvt.getArg()).getBuffer()));


	}

	public static void testKeyChangesDuringKeyServerChange() throws Exception {
		// create peers and server
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
		Event serverEvent = createViewChange(1, serverAddress);
		server.up(new Event(Event.TMP_VIEW, serverEvent.getArg()));
		server.up(serverEvent);

		Address peerAddress=new MockAddress("peer");
		peer.setLocal_addr(peerAddress);
		MockObserver peerObserver=new MockObserver();
		peer.setObserver(peerObserver);
		peer.keyServer=false;

		updateViewFor(peer, server, serverObserver, serverEvent, peerObserver);


		// set up peer2 with server as key server
		Address peer2Address=new MockAddress("peer2");
		peer2.setLocal_addr(peer2Address);
		MockObserver peer2Observer=new MockObserver();
		peer2.setObserver(peer2Observer);
		peer2.keyServer=false;
		updateViewFor(peer2, server, serverObserver, serverEvent, peer2Observer);

		Assert.assertEquals(server.getSymState().getSecretKey(), peer.getSymState().getSecretKey());
		Assert.assertEquals(server.getSymState().getSecretKey(), peer2.getSymState().getSecretKey());

		Event viewChange2 = createViewChange(2, peer2Address);
		peer2.up(new Event(Event.TMP_VIEW, viewChange2.getArg()));
		peer2.up(viewChange2);

		updateViewFor(peer, peer2, peer2Observer, viewChange2, peerObserver);

		Assert.assertFalse(server.getSymState().getSecretKey().equals(peer.getSymState().getSecretKey()));
		Assert.assertEquals(peer.getSymState().getSecretKey(), peer2.getSymState().getSecretKey());

	}

	public static void testSymmetricKeyIsChangedOnViewChange() throws Exception{

		ENCRYPT server=new ENCRYPT();
		server.changeKeysOnViewChange=true;
		MockObserver serverObserver=new MockObserver();
		server.setObserver(serverObserver);
		Address serverAddress=new MockAddress("server");
		server.setLocal_addr(serverAddress);
		server.init();

		//	set the server up as key server
		Event initalView = createViewChange(1, serverAddress);
		server.up(new Event(Event.TMP_VIEW, initalView.getArg()));
		server.up(initalView);

		SecretKey key = server.getSymState().getSecretKey();

		//	Update the view with new member
		Address peerAddress=new MockAddress("peer");
		Event updatedView = createViewChange(2, serverAddress, peerAddress);
		server.up(new Event(Event.TMP_VIEW, updatedView.getArg()));
		server.up(updatedView);

		SecretKey keyAfterViewChange = server.getSymState().getSecretKey();

		Util.assertFalse(key.equals(keyAfterViewChange));

	}
	public static void testMessagesNotPassedUpDuringQueuingUp() throws Exception{

		ENCRYPT node=new ENCRYPT();
		node.setUpProtocol(new FailOnBatchProtocol());
		node.changeKeysOnViewChange=true;
		Address serverAddress=new MockAddress("server");
		Address peerAddress=new MockAddress("peer");
		node.setLocal_addr(peerAddress);
		node.init();

		// Send up view to activate queuing
		Event initalView = createViewChange(1, serverAddress, peerAddress);
		node.up(initalView);

		Message msg = new Message(serverAddress);
		msg.setBuffer("hello".getBytes());
		EncryptHeader hdr=new EncryptHeader(EncryptHeader.ENCRYPT, "N/A");
		msg.putHeader(node.getId(), hdr);
		MessageBatch batch = new MessageBatch(Collections.singletonList(msg));

		node.up(batch);



	}

	private static void updateViewFor(ENCRYPT peer, ENCRYPT keyServer,
			MockObserver serverObserver, Event serverEvent,
			MockObserver peerObserver) {
		peer.up(serverEvent);
		Event peerKeyRequest = getLatestDownMessage(peerObserver);
		keyServer.up(peerKeyRequest);
		Event serverKeyToPeer = getLatestDownMessage(serverObserver);
		peer.up(serverKeyToPeer);
	}

	private static Event createViewChange(int id, Address serverAddress, Address...addresses ) {
		List<Address> serverVector=new ArrayList<Address>() ;
		serverVector.add(serverAddress);
		serverVector.addAll(Arrays.asList(addresses));
		View tempView=new View(new ViewId(serverAddress, id), serverVector);
		return new Event(Event.VIEW_CHANGE, tempView);
	}


	static Event getLatestDownMessage(MockObserver observer) {
		TreeMap<String, Event> map = observer.getDownMessages();
		if ( map.isEmpty()) {
			return null;
		} else {
			return map.lastEntry().getValue();
		}
	}

	static Event getLatestUpMessage(MockObserver observer) {
		TreeMap<String, Event> map = observer.getUpMessages();
		if ( map.isEmpty()) {
			return null;
		} else {
			return map.lastEntry().getValue();
		}
	}

	static class MockObserver implements ENCRYPT.Observer {
		private TreeMap<String, Event> upMessages=new TreeMap<String, Event>();
		private TreeMap<String, Event> downMessages=new TreeMap<String, Event>();
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

		@Override
		public void up(Event evt) {
			storeUp(evt);
		}

		public void setProtocol(Protocol prot) {
		}

		@Override
		public void passUp(Event evt) {
			storeUp(evt);
		}

		@Override
		public void down(Event evt) {
		}

		@Override
		public void passDown(Event evt) {
			storeDown(evt);
		}

		protected TreeMap<String, Event> getUpMessages() {
			return upMessages;
		}

		protected void setUpMessages(TreeMap<String, Event> upMessages) {
			this.upMessages=upMessages;
		}

		protected TreeMap<String, Event> getDownMessages() {
			return downMessages;
		}

		protected void setDownMessages(TreeMap<String, Event> downMessages) {
			this.downMessages=downMessages;
		}
	}

	static class MockAddress implements Address {
		private static final long serialVersionUID=990515143342934541L;
		String name;

		public MockAddress(String name) {
			this.name=name;
		}

		public MockAddress() {
		}

		@Override
		public int size() {
			return 0;
		}

		@Override
		public int compareTo(Address o) {
			return -1;
		}

		@Override
		public boolean equals(Object obj) {
			MockAddress address=(MockAddress)obj;
			return address.name.equals(this.name);
		}

		@Override
		public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
		}

		@Override
		public void writeExternal(ObjectOutput out) throws IOException {
		}

		@Override
		public void writeTo(DataOutput out) throws Exception {
		}

		@Override
		public void readFrom(DataInput in) throws Exception {
		}
	}

	private static class FailOnBatchProtocol extends FRAG {

		@Override
		public Object up(Event evt) {
			return null;
		}

		@Override
		public void up(MessageBatch batch) {
			throw new IllegalArgumentException("Protocol does not accept message batches");
		}

	}
}
