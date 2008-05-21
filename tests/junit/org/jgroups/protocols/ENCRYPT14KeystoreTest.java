/*
 * Created on 04-Jul-2004
 *
 * To change the template for this generated file go to
 * Window - Preferences - Java - Code Generation - Code and Comments
 */
package org.jgroups.protocols;

import junit.framework.TestCase;
import org.jgroups.Address;
import org.jgroups.Event;
import org.jgroups.Message;
import org.jgroups.stack.Protocol;

import javax.crypto.Cipher;
import java.io.*;
import java.security.MessageDigest;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

/**
 * @author xenephon
 * 
 * To change the template for this generated type comment go to Window -
 * Preferences - Java - Code Generation - Code and Comments
 */
public class ENCRYPT14KeystoreTest extends TestCase {

    public void testInitWrongKeystoreProperties() {
        Properties props=new Properties();
        String defaultKeystore="unkownKeystore.keystore";
        props.put("key_store_name", defaultKeystore);
        ENCRYPT encrypt=new ENCRYPT();
        encrypt.setProperties(props);
        try {
            encrypt.init();
        }
        catch(Exception e) {
            System.out.println("didn't find incorrect keystore (as expected): " + e.getMessage());
            assertEquals("Unable to load keystore " + defaultKeystore
                         + " ensure file is on classpath", e.getMessage());
        }
    }

    public void testInitKeystoreProperties() throws Exception {

        Properties props=new Properties();
        String defaultKeystore="defaultStore.keystore";
        props.put("key_store_name", defaultKeystore);

        //javax.
        ENCRYPT encrypt=new ENCRYPT();
        encrypt.setProperties(props);
        encrypt.init();
        assertNotNull(encrypt.getSymDecodingCipher());
        assertNotNull(encrypt.getSymEncodingCipher());

    }

    public void testMessageDownEncode() throws Exception {
        // initialise the encryption
        Properties props=new Properties();
        String defaultKeystore="defaultStore.keystore";
        props.put("key_store_name", defaultKeystore);

        //javax.
        ENCRYPT encrypt=new ENCRYPT();
        encrypt.setProperties(props);
        encrypt.init();

        // use a second instance so we know we are not accidentally using internal key
        Properties props2=new Properties();
        props2.put("key_store_name", defaultKeystore);
        //		javax.
        ENCRYPT encrypt2=new ENCRYPT();
        encrypt2.setProperties(props2);
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
        assertNotSame(encText, messageText);
        Cipher cipher=encrypt2.getSymDecodingCipher();
        byte[] decodedBytes=cipher.doFinal(sentMsg.getBuffer());
        String temp=new String(decodedBytes);
        System.out.println("decoded text:" + temp);
        assertEquals(temp, messageText);

    }

    public void testMessageUpDecode() throws Exception {
        // initialise the encryption
        Properties props=new Properties();
        String defaultKeystore="defaultStore.keystore";
        props.put("key_store_name", defaultKeystore);

        //javax.
        ENCRYPT encrypt=new ENCRYPT();
        encrypt.setProperties(props);
        encrypt.init();

        // use a second instance so we know we are not accidentally using internal key
        Properties props2=new Properties();
        props2.put("key_store_name", defaultKeystore);
        //		javax.
        ENCRYPT encrypt2=new ENCRYPT();
        encrypt2.setProperties(props2);
        encrypt2.init();

        MockObserver observer=new MockObserver();
        encrypt.setObserver(observer);

        encrypt.keyServer=true;
        String messageText="hello this is a test message";
        Cipher cipher=encrypt2.getSymEncodingCipher();
        byte[] encodedBytes=cipher.doFinal(messageText.getBytes());
        assertNotSame(new String(encodedBytes), messageText);

        MessageDigest digest=MessageDigest.getInstance("MD5");
        digest.reset();
        digest.update(encrypt.getDesKey().getEncoded());

        String symVersion=new String(digest.digest(), "UTF-8");

        Message msg=new Message(null, null, encodedBytes);
        msg.putHeader(ENCRYPT.EncryptHeader.KEY,
                      new ENCRYPT.EncryptHeader(ENCRYPT.EncryptHeader.ENCRYPT, symVersion));
        Event event=new Event(Event.MSG, msg);
        encrypt.up(event);
        Message rcvdMsg=(Message)((Event)observer.getUpMessages().get("message0")).getArg();
        String decText=new String(rcvdMsg.getBuffer());

        assertEquals(decText, messageText);

    }

    public void testMessageUpWrongKey() throws Exception {
        // initialise the encryption
        Properties props=new Properties();
        String defaultKeystore="defaultStore.keystore";
        String defaultKeystore2="defaultStore2.keystore";
        props.put("key_store_name", defaultKeystore);

        //javax.
        ENCRYPT encrypt=new ENCRYPT();
        encrypt.setProperties(props);
        encrypt.init();
        // use a second instance so we know we are not accidentally using internal key

        Properties props2=new Properties();
        ENCRYPT encrypt2=new ENCRYPT();
        props2.setProperty("key_store_name", defaultKeystore2);

        encrypt2.setProperties(props2);
        encrypt2.init();

        MockObserver observer=new MockObserver();
        encrypt.setObserver(observer);

        encrypt.keyServer=true;
        String messageText="hello this is a test message";
        Cipher cipher=encrypt2.getSymEncodingCipher();
        byte[] encodedBytes=cipher.doFinal(messageText.getBytes());
        assertNotSame(new String(encodedBytes), messageText);

        MessageDigest digest=MessageDigest.getInstance("MD5");
        digest.reset();
        digest.update(encrypt2.getDesKey().getEncoded());

        String symVersion=new String(digest.digest());

        Message msg=new Message(null, null, encodedBytes);
        msg.putHeader(ENCRYPT.EncryptHeader.KEY,
                      new ENCRYPT.EncryptHeader(ENCRYPT.EncryptHeader.ENCRYPT, symVersion));
        Event event=new Event(Event.MSG, msg);
        encrypt.up(event);
        assertEquals(0, observer.getUpMessages().size());

    }

    public void testMessageUpNoEncryptHeader() throws Exception {
        // initialise the encryption
        Properties props=new Properties();
        String defaultKeystore="defaultStore.keystore";
        props.put("key_store_name", defaultKeystore);

        //javax.
        ENCRYPT encrypt=new ENCRYPT();
        encrypt.setProperties(props);
        encrypt.init();

        // use a second instance so we know we are not accidentally using internal key
        Properties props2=new Properties();
        props2.put("key_store_name", defaultKeystore);
        //		javax.
        ENCRYPT encrypt2=new ENCRYPT();
        encrypt2.setProperties(props2);
        encrypt2.init();

        MockObserver observer=new MockObserver();
        encrypt.setObserver(observer);

        encrypt.keyServer=true;
        String messageText="hello this is a test message";
        Cipher cipher=encrypt2.getSymEncodingCipher();
        byte[] encodedBytes=cipher.doFinal(messageText.getBytes());
        assertNotSame(new String(encodedBytes), messageText);

        Message msg=new Message(null, null, encodedBytes);
        Event event=new Event(Event.MSG, msg);
        encrypt.up(event);
        assertEquals(0, observer.getUpMessages().size());

    }

    public void testEventUpNoMessage() throws Exception {
        // initialise the encryption
        Properties props=new Properties();
        String defaultKeystore="defaultStore.keystore";
        props.put("key_store_name", defaultKeystore);

        //javax.
        ENCRYPT encrypt=new ENCRYPT();
        encrypt.setProperties(props);
        encrypt.init();

        MockObserver observer=new MockObserver();
        encrypt.setObserver(observer);
        encrypt.keyServer=true;

        Event event=new Event(Event.MSG, null);
        encrypt.up(event);
        assertEquals(1, observer.getUpMessages().size());

    }

    public void testMessageUpNoBuffer() throws Exception {
        // initialise the encryption
        Properties props=new Properties();
        String defaultKeystore="defaultStore.keystore";
        props.put("key_store_name", defaultKeystore);

        //javax.
        ENCRYPT encrypt=new ENCRYPT();
        encrypt.setProperties(props);
        encrypt.init();

        MockObserver observer=new MockObserver();
        encrypt.setObserver(observer);
        encrypt.keyServer=true;
        Message msg=new Message(null, null, null);
        Event event=new Event(Event.MSG, msg);
        encrypt.up(event);
        assertEquals(1, observer.getUpMessages().size());
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
            System.out.println("passdown:" + evt.toString());
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

    static class MockAddress implements Address {

        private static final long serialVersionUID=-2044466632514705356L;

        /* (non-Javadoc)
         * @see org.jgroups.Address#isMulticastAddress()
         */
        public boolean isMulticastAddress() {
            return false;
        }

        public int size() {
            return 0;
        }

        /* (non-Javadoc)
         * @see java.io.Externalizable#readExternal(java.io.ObjectInput)
         */
        public void readExternal(ObjectInput in) throws IOException,ClassNotFoundException {

        }

        /* (non-Javadoc)
         * @see java.io.Externalizable#writeExternal(java.io.ObjectOutput)
         */
        public void writeExternal(ObjectOutput out) throws IOException {}

        public void writeTo(DataOutputStream out) throws IOException {
            ;
        }

        public void readFrom(DataInputStream in) throws IOException,
                                                IllegalAccessException,
                                                InstantiationException {
            ;
        }

        /* (non-Javadoc)
         * @see java.lang.Comparable#compareTo(java.lang.Object)
         */
        public int compareTo(Object o) {
            return -1;
        }

    }
}
