/*
 * Created on 04-Jul-2004
 */
package org.jgroups.protocols;


import org.jgroups.Address;
import org.jgroups.Event;
import org.jgroups.Global;
import org.jgroups.Message;
import org.jgroups.stack.Protocol;
import org.testng.annotations.Test;

import javax.crypto.Cipher;
import java.io.*;
import java.security.MessageDigest;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

/**
 * @author xenephon
 */
@Test(groups=Global.FUNCTIONAL, sequential=false)
public class ENCRYPT14KeystoreTest {


    public static void testInitWrongKeystoreProperties() {        
        ENCRYPT encrypt=new ENCRYPT();
        encrypt.keyStoreName = "unkownKeystore.keystore";
        try {
            encrypt.init();
        }
        catch(Exception e) {
            System.out.println("didn't find incorrect keystore (as expected): " + e.getMessage());
            assert e.getMessage().equals("Unable to load keystore " +  "unkownKeystore.keystore" + " ensure file is on classpath");
        }
    }

    public static void testInitKeystoreProperties() throws Exception {

        //javax.
        ENCRYPT encrypt=new ENCRYPT();
        encrypt.keyStoreName = "defaultStore.keystore";
        encrypt.init();
        assert encrypt.getSymDecodingCipher() != null;
        assert encrypt.getSymEncodingCipher() != null;

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
        Cipher cipher=encrypt2.getSymDecodingCipher();
        byte[] decodedBytes=cipher.doFinal(sentMsg.getBuffer());
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
        Cipher cipher=encrypt2.getSymEncodingCipher();
        byte[] encodedBytes=cipher.doFinal(messageText.getBytes());
        assert !new String(encodedBytes).equals(messageText);

        MessageDigest digest=MessageDigest.getInstance("MD5");
        digest.reset();
        digest.update(encrypt.getDesKey().getEncoded());

        String symVersion=new String(digest.digest(), "UTF-8");

        Message msg=new Message(null, null, encodedBytes);
        msg.putHeader(ENCRYPT.EncryptHeader.KEY, new ENCRYPT.EncryptHeader(ENCRYPT.EncryptHeader.ENCRYPT, symVersion));
        Event event=new Event(Event.MSG, msg);
        encrypt.up(event);
        Message rcvdMsg=(Message)((Event)observer.getUpMessages().get("message0")).getArg();
        String decText=new String(rcvdMsg.getBuffer());

        assert decText.equals(messageText);

    }

    public static void testMessageUpWrongKey() throws Exception {
        // initialise the encryption       

        //javax.
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
        Cipher cipher=encrypt2.getSymEncodingCipher();
        byte[] encodedBytes=cipher.doFinal(messageText.getBytes());
        assert !new String(encodedBytes).equals(messageText);

        MessageDigest digest=MessageDigest.getInstance("MD5");
        digest.reset();
        digest.update(encrypt2.getDesKey().getEncoded());

        String symVersion=new String(digest.digest());

        Message msg=new Message(null, null, encodedBytes);
        msg.putHeader(ENCRYPT.EncryptHeader.KEY, new ENCRYPT.EncryptHeader(ENCRYPT.EncryptHeader.ENCRYPT, symVersion));
        Event event=new Event(Event.MSG, msg);
        encrypt.up(event);
        assert observer.getUpMessages().isEmpty();

    }

    public static void testMessageUpNoEncryptHeader() throws Exception {
       
        //javax.
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
        Cipher cipher=encrypt2.getSymEncodingCipher();
        byte[] encodedBytes=cipher.doFinal(messageText.getBytes());
        assert !new String(encodedBytes).equals(messageText);

        Message msg=new Message(null, null, encodedBytes);
        Event event=new Event(Event.MSG, msg);
        encrypt.up(event);
        assert observer.getUpMessages().isEmpty();


    }

    public static void testEventUpNoMessage() throws Exception {
     
        //javax.
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
        private static final long serialVersionUID=-2044466632514705356L;

        public boolean isMulticastAddress() {
            return false;
        }

        public int size() {
            return 0;
        }

        public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {

        }

        public void writeExternal(ObjectOutput out) throws IOException {
        }

        public void writeTo(DataOutputStream out) throws IOException {
            ;
        }

        public void readFrom(DataInputStream in) throws IOException, IllegalAccessException, InstantiationException {
            ;
        }

        public int compareTo(Address o) {
            return -1;
        }


    }
}
