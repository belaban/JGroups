/*
 * Created on 04-Jul-2004
 */
package org.jgroups.protocols;


import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.security.MessageDigest;
import java.util.HashMap;
import java.util.Map;

import org.jgroups.Address;
import org.jgroups.Event;
import org.jgroups.Global;
import org.jgroups.Message;
import org.jgroups.conf.ClassConfigurator;
import org.jgroups.protocols.ENCRYPT.SymmetricCipherState;
import org.jgroups.stack.Protocol;
import org.testng.annotations.Test;

/**
 * @author xenephon
 */
@Test(groups=Global.FUNCTIONAL, sequential=false)
public class ENCRYPT14KeystoreTest {

	static final short ENCRYPT_ID=ClassConfigurator.getProtocolId(ENCRYPT.class);

	public static void testInitWrongKeystoreProperties() {
		ENCRYPT encrypt=new ENCRYPT();
		encrypt.keyStoreName = "unkownKeystore.keystore";
		try {
			encrypt.init();
		}
		catch(Exception e) {
			System.out.println("didn't find incorrect keystore (as expected): " + e.getMessage());
		}
	}

	public static void testInitKeystoreProperties() throws Exception {
		ENCRYPT encrypt=new ENCRYPT();
		encrypt.keyStoreName = "defaultStore.keystore";
		encrypt.init();
		assert encrypt.getSymState() != null;

	}

	public static void testMessageDownEncode() throws Exception {
		ENCRYPT encrypt=new ENCRYPT();
		encrypt.keyStoreName = "defaultStore.keystore";
		encrypt.init();


		ENCRYPT encrypt2=new ENCRYPT();
		encrypt2.keyStoreName = "defaultStore.keystore";
		encrypt2.init();

		MockObserver observer=new MockObserver();
		encrypt.setObserver(observer);

		encrypt.keyServer=true;
		String messageText="hello this is a test message";
		Message msg=new Message(null, null, messageText.getBytes());

		Event event=new Event(Event.MSG, msg);
		encrypt.down(event);
		Message sentMsg=(Message)((Event)observer.getDownMessages().get("message0")).getArg();
		String encText=new String(sentMsg.getBuffer());
		assert !encText.equals(messageText);
		SymmetricCipherState state=encrypt2.getSymState();
		byte[] decodedBytes=state.decryptMessage(sentMsg, false).getBuffer();
		String temp=new String(decodedBytes);
		System.out.println("decoded text:" + temp);
		assert temp.equals(messageText);

	}


	public static void testMessageUpDecode() throws Exception {
		ENCRYPT encrypt=new ENCRYPT();
		encrypt.keyStoreName = "defaultStore.keystore";
		encrypt.init();


		ENCRYPT encrypt2=new ENCRYPT();
		encrypt2.keyStoreName = "defaultStore.keystore";
		encrypt2.init();

		MockObserver observer=new MockObserver();
		encrypt.setObserver(observer);

		encrypt.keyServer=true;
		String messageText="hello this is a test message";
		SymmetricCipherState state=encrypt2.getSymState();
		byte[] encodedBytes=state.encryptMessage(messageText.getBytes(), 0	, messageText.getBytes().length);
		assert !new String(encodedBytes).equals(messageText);

		MessageDigest digest=MessageDigest.getInstance("MD5");
		digest.reset();
		digest.update(encrypt.getSymState().getSecretKey().getEncoded());

		//String symVersion=new String(digest.digest(), "UTF-8");
		String symVersion=ENCRYPT.byteArrayToHexString(digest.digest());

		Message msg=new Message(null, null, encodedBytes);
		msg.putHeader(ENCRYPT_ID, new ENCRYPT.EncryptHeader(ENCRYPT.EncryptHeader.ENCRYPT, symVersion));
		Event event=new Event(Event.MSG, msg);
		encrypt.up(event);
		Message rcvdMsg=(Message)((Event)observer.getUpMessages().get("message0")).getArg();
		String decText=new String(rcvdMsg.getBuffer());

		assert decText.equals(messageText);

	}

	public static void testMessageUpWrongKey() throws Exception {
		ENCRYPT encrypt=new ENCRYPT();
		encrypt.keyStoreName = "defaultStore.keystore";
		encrypt.init();
		// use a second instance so we know we are not accidentally using internal key

		ENCRYPT encrypt2=new ENCRYPT();

		encrypt.keyStoreName = "defaultStore2.keystore";
		encrypt2.init();

		MockObserver observer=new MockObserver();
		encrypt.setObserver(observer);

		encrypt.keyServer=true;
		String messageText="hello this is a test message";
		SymmetricCipherState state=encrypt2.getSymState();
		byte[] encodedBytes=state.encryptMessage(messageText.getBytes(), 0	, messageText.getBytes().length);
		assert !new String(encodedBytes).equals(messageText);

		Message msg=new Message(null, null, encodedBytes);
		msg.putHeader(ENCRYPT_ID, new ENCRYPT.EncryptHeader(ENCRYPT.EncryptHeader.ENCRYPT, state.getSymVersion()));
		Event event=new Event(Event.MSG, msg);
		encrypt.up(event);
		assert observer.getUpMessages().isEmpty();

	}

	public static void testMessageUpNoEncryptHeader() throws Exception {
		ENCRYPT encrypt=new ENCRYPT();
		encrypt.keyStoreName = "defaultStore.keystore";
		encrypt.init();

		ENCRYPT encrypt2=new ENCRYPT();
		encrypt.keyStoreName = "defaultStore.keystore";
		encrypt2.init();

		MockObserver observer=new MockObserver();
		encrypt.setObserver(observer);

		encrypt.keyServer=true;
		String messageText="hello this is a test message";
		SymmetricCipherState state=encrypt2.getSymState();
		byte[] encodedBytes=state.encryptMessage(messageText.getBytes(), 0, messageText.getBytes().length);
		assert !new String(encodedBytes).equals(messageText);

		Message msg=new Message(null, null, encodedBytes);
		Event event=new Event(Event.MSG, msg);
		encrypt.up(event);
		assert observer.getUpMessages().isEmpty();


	}

	public static void testEventUpNoMessage() throws Exception {
		ENCRYPT encrypt=new ENCRYPT();
		encrypt.keyStoreName = "defaultStore.keystore";
		encrypt.init();

		MockObserver observer=new MockObserver();
		encrypt.setObserver(observer);
		encrypt.keyServer=true;

		Event event=new Event(Event.MSG, null);
		encrypt.up(event);
		assert observer.getUpMessages().size() == 1;


	}

	public static void testMessageUpNoBuffer() throws Exception {
		ENCRYPT encrypt=new ENCRYPT();
		encrypt.keyStoreName = "defaultStore.keystore";
		encrypt.init();

		MockObserver observer=new MockObserver();
		encrypt.setObserver(observer);
		encrypt.keyServer=true;
		Message msg=new Message(null, null, null);
		Event event=new Event(Event.MSG, msg);
		encrypt.up(event);
		assert observer.getUpMessages().size() == 1;
	}

	static class MockObserver implements ENCRYPT.Observer {

		private Map upMessages=new HashMap();
		private Map downMessages=new HashMap();
		private int counter=0;


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

		@Override
		public int size() {
			return 0;
		}

		@Override
		public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
		}

		@Override
		public void writeExternal(ObjectOutput out) throws IOException {
		}

		@Override
		public void writeTo(DataOutput out) throws Exception {
			;
		}

		@Override
		public void readFrom(DataInput in) throws Exception {
			;
		}

		@Override
		public int compareTo(Address o) {
			return -1;
		}


	}
}
