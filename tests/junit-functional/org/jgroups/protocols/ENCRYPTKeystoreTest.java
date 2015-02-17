/*
 * Created on 04-Jul-2004
 */
package org.jgroups.protocols;


import org.jgroups.Event;
import org.jgroups.Global;
import org.jgroups.Message;
import org.jgroups.conf.ClassConfigurator;
import org.jgroups.stack.Protocol;
import org.jgroups.util.MessageBatch;
import org.testng.annotations.Test;

import javax.crypto.Cipher;
import java.security.MessageDigest;
import java.util.HashMap;
import java.util.Map;

/**
 * @author xenephon
 * @author Bela Ban
 */
@Test(groups=Global.FUNCTIONAL, sequential=false)
public class ENCRYPTKeystoreTest {

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
        assert encrypt.getSymDecodingCipher() != null;
        assert encrypt.getSymEncodingCipher() != null;

    }

    public static void testMessageDownEncode() throws Exception {
        ENCRYPT encrypt=create("defaultStore.keystore"), encrypt2=create("defaultStore.keystore");
        MockProtocol observer=new MockProtocol();
        encrypt.setDownProtocol(observer);

        encrypt.keyServer=true;
        String messageText="hello this is a test message";
        Message msg=new Message(null, messageText.getBytes());

        encrypt.down(new Event(Event.MSG, msg));
        Message sentMsg=(Message)observer.getDownMessages().get("message0").getArg();
        String encText=new String(sentMsg.getBuffer());
        assert !encText.equals(messageText);
        Cipher cipher=encrypt2.getSymDecodingCipher();
        byte[] decodedBytes=cipher.doFinal(sentMsg.getBuffer());
        String temp=new String(decodedBytes);
        System.out.println("decoded text:" + temp);
        assert temp.equals(messageText);

    }


    public static void testMessageUpDecode() throws Exception {
        ENCRYPT encrypt=create("defaultStore.keystore"), encrypt2=create("defaultStore.keystore");
        
        MockProtocol observer=new MockProtocol();
        encrypt.setUpProtocol(observer);

        encrypt.keyServer=true;
        String messageText="hello this is a test message";
        Cipher cipher=encrypt2.getSymEncodingCipher();
        byte[] encodedBytes=cipher.doFinal(messageText.getBytes());
        assert !new String(encodedBytes).equals(messageText);

        MessageDigest digest=MessageDigest.getInstance("MD5");
        digest.reset();
        digest.update(encrypt.getDesKey().getEncoded());

        byte[] symVersion=digest.digest();
        Message msg=new Message(null, encodedBytes)
          .putHeader(ENCRYPT_ID, new ENCRYPT.EncryptHeader(ENCRYPT.EncryptHeader.ENCRYPT, symVersion));
        encrypt.up(new Event(Event.MSG, msg));
        Message rcvdMsg=(Message)observer.getUpMessages().get("message0").getArg();
        String decText=new String(rcvdMsg.getBuffer());
        assert decText.equals(messageText);

    }

    public static void testMessageUpWrongKey() throws Exception {
        ENCRYPT encrypt=create("defaultStore.keystore"), encrypt2=create("defaultStore2.keystore");
        MockProtocol observer=new MockProtocol();
        encrypt.setUpProtocol(observer);

        encrypt.keyServer=true;
        String messageText="hello this is a test message";
        Cipher cipher=encrypt2.getSymEncodingCipher();
        byte[] encodedBytes=cipher.doFinal(messageText.getBytes());
        assert !new String(encodedBytes).equals(messageText);

        MessageDigest digest=MessageDigest.getInstance("MD5");
        digest.reset();
        digest.update(encrypt2.getDesKey().getEncoded());

        byte[] symVersion=digest.digest();

        Message msg=new Message(null, null, encodedBytes)
          .putHeader(ENCRYPT_ID, new ENCRYPT.EncryptHeader(ENCRYPT.EncryptHeader.ENCRYPT, symVersion));
        encrypt.up(new Event(Event.MSG, msg));
        assert observer.getUpMessages().isEmpty();
    }

    public static void testMessageUpNoEncryptHeader() throws Exception {
        ENCRYPT encrypt=create("defaultStore.keystore"), encrypt2=create("defaultStore.keystore");
        MockProtocol observer=new MockProtocol();
        encrypt.setUpProtocol(observer);

        encrypt.keyServer=true;
        String messageText="hello this is a test message";
        Cipher cipher=encrypt2.getSymEncodingCipher();
        byte[] encodedBytes=cipher.doFinal(messageText.getBytes());
        assert !new String(encodedBytes).equals(messageText);

        Message msg=new Message(null, encodedBytes);
        encrypt.up(new Event(Event.MSG, msg));
        assert observer.getUpMessages().size() == 1;
    }

    public static void testEventUpNoMessage() throws Exception {
        ENCRYPT encrypt=create("defaultStore.keystore");
        MockProtocol observer=new MockProtocol();
        encrypt.setUpProtocol(observer);
        encrypt.keyServer=true;

        encrypt.up(new Event(Event.MSG, null));
        assert observer.getUpMessages().size() == 1;


    }

    public static void testMessageUpNoBuffer() throws Exception {
        ENCRYPT encrypt=create("defaultStore.keystore");
        MockProtocol observer=new MockProtocol();
        encrypt.setUpProtocol(observer);
        encrypt.keyServer=true;
        encrypt.up(new Event(Event.MSG, new Message()));
        assert observer.getUpMessages().size() == 1;
    }

    public void testEncryptEntireMessage() throws Exception {
        ENCRYPT encrypt=create("defaultStore.keystore");
        encrypt.keyServer=true;
        encrypt.setValue("encrypt_entire_message",true);
        Message msg=new Message(null, "hello world".getBytes()).putHeader((short)1, new TpHeader("cluster"));
        MockProtocol mock=new MockProtocol();
        encrypt.setDownProtocol(mock);
        encrypt.down(new Event(Event.MSG, msg));

        Message encrypted_msg=(Message)mock.getDownMessages().get("message0").getArg();

        encrypt.setDownProtocol(null);
        encrypt.setUpProtocol(mock);
        encrypt.up(new Event(Event.MSG, encrypted_msg));

        Message decrypted_msg=(Message)mock.getUpMessages().get("message1").getArg();
        String temp=new String(decrypted_msg.getBuffer());
        assert "hello world".equals(temp);
    }

    protected static ENCRYPT create(String keystore) throws Exception {
        ENCRYPT encrypt=new ENCRYPT();
        encrypt.keyStoreName = keystore;
        encrypt.init();
        return encrypt;
    }


    protected static class MockProtocol extends Protocol {
        private final Map<String,Event> upMessages=new HashMap<>();
        private final Map<String,Event> downMessages=new HashMap<>();
        private int                     counter;

        public Map<String,Event> getDownMessages() {return downMessages;}
        public Map<String,Event> getUpMessages()   {return upMessages;}

        public Object down(Event evt) {
            downMessages.put("message" + counter++, evt);
            return null;
        }

        public Object up(Event evt) {
            upMessages.put("message" + counter++, evt);
            return null;
        }

        public void up(MessageBatch batch) {throw new UnsupportedOperationException();}
    }



}
