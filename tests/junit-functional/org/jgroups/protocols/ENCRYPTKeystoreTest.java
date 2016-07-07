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

import java.security.MessageDigest;
import java.util.HashMap;
import java.util.Map;

/**
 * @author xenephon
 * @author Bela Ban
 */
@Test(groups=Global.FUNCTIONAL, sequential=false)
public class ENCRYPTKeystoreTest {

    static final short ENCRYPT_ID=ClassConfigurator.getProtocolId(SYM_ENCRYPT.class);

    public void testInitWrongKeystoreProperties() {
        SYM_ENCRYPT encrypt=new SYM_ENCRYPT().keystoreName("unkownKeystore.keystore");
        try {
            encrypt.init();
        }
        catch(Exception e) {
            System.out.println("didn't find incorrect keystore (as expected): " + e.getMessage());
        }
    }

    public void testInitKeystoreProperties() throws Exception {
        SYM_ENCRYPT encrypt=new SYM_ENCRYPT().keystoreName("defaultStore.keystore");
        encrypt.init();
    }

    public void testMessageDownEncode() throws Exception {
        SYM_ENCRYPT encrypt=create("defaultStore.keystore");
        MockProtocol observer=new MockProtocol();
        encrypt.setDownProtocol(observer);

        String messageText="hello this is a test message";
        Message msg=new Message(null, messageText.getBytes());

        encrypt.down(new Event(Event.MSG, msg));
        Message sentMsg=(Message)observer.getDownMessages().get("message0").getArg();
        String encText=new String(sentMsg.getBuffer());
        assert !encText.equals(messageText);
        byte[] decodedBytes=encrypt.code(sentMsg.getRawBuffer(), sentMsg.getOffset(), sentMsg.getLength(), true);
        String temp=new String(decodedBytes);
        System.out.printf("decoded text: '%s'\n", temp);
        assert temp.equals(messageText) : String.format("sent: '%s', decoded: '%s'", messageText, temp);
    }


    public void testMessageUpDecode() throws Exception {
        SYM_ENCRYPT encrypt=create("defaultStore.keystore"), encrypt2=create("defaultStore.keystore");
        
        MockProtocol observer=new MockProtocol();
        encrypt.setUpProtocol(observer);

        String messageText="hello this is a test message";
        byte[] bytes=messageText.getBytes();
        byte[] encodedBytes=encrypt2.code(bytes, 0, bytes.length, false);
        assert !new String(encodedBytes).equals(messageText);

        MessageDigest digest=MessageDigest.getInstance("MD5");
        digest.reset();
        digest.update(encrypt.secretKey().getEncoded());

        byte[] symVersion=digest.digest();
        Message msg=new Message(null, encodedBytes).putHeader(ENCRYPT_ID, new EncryptHeader(EncryptHeader.ENCRYPT, symVersion));
        encrypt.up(new Event(Event.MSG, msg));
        Message rcvdMsg=(Message)observer.getUpMessages().get("message0").getArg();
        String decText=new String(rcvdMsg.getBuffer());
        assert decText.equals(messageText);

    }

    public void testMessageUpWrongKey() throws Exception {
        SYM_ENCRYPT encrypt=create("defaultStore.keystore"), encrypt2=create("defaultStore2.keystore");
        MockProtocol observer=new MockProtocol();
        encrypt.setUpProtocol(observer);

        String messageText="hello this is a test message";
        byte[] bytes=messageText.getBytes();
        byte[] encodedBytes=encrypt2.code(bytes, 0, bytes.length, false);
        assert !new String(encodedBytes).equals(messageText);

        MessageDigest digest=MessageDigest.getInstance("MD5");
        digest.reset();
        digest.update(encrypt2.secretKey().getEncoded());

        byte[] symVersion=digest.digest();

        Message msg=new Message(null, encodedBytes).putHeader(ENCRYPT_ID, new EncryptHeader(EncryptHeader.ENCRYPT, symVersion));
        encrypt.up(new Event(Event.MSG, msg));
        assert observer.getUpMessages().isEmpty();
    }

    public void testMessageUpNoEncryptHeader() throws Exception {
        SYM_ENCRYPT encrypt=create("defaultStore.keystore"), encrypt2=create("defaultStore.keystore");
        MockProtocol observer=new MockProtocol();
        encrypt.setUpProtocol(observer);
        String messageText="hello this is a test message";
        byte[] bytes=messageText.getBytes();
        byte[] encodedBytes=encrypt2.code(bytes, 0, bytes.length, false);
        assert !new String(encodedBytes).equals(messageText);
        Message msg=new Message(null, encodedBytes);
        encrypt.up(new Event(Event.MSG, msg));
        assert observer.getUpMessages().isEmpty();
    }

    public void testEventUpNoMessage() throws Exception {
        SYM_ENCRYPT encrypt=create("defaultStore.keystore");
        MockProtocol observer=new MockProtocol();
        encrypt.setUpProtocol(observer);
        encrypt.up(new Event(Event.MSG, null));
        assert observer.getUpMessages().isEmpty();
    }

    public void testMessageUpNoBuffer() throws Exception {
        SYM_ENCRYPT encrypt=create("defaultStore.keystore");
        MockProtocol observer=new MockProtocol();
        encrypt.setUpProtocol(observer);
        encrypt.up(new Event(Event.MSG, new Message().putHeader(ENCRYPT_ID, new EncryptHeader(EncryptHeader.ENCRYPT, "bla".getBytes()))));
        assert observer.getUpMessages().isEmpty();
    }

    public void testEncryptEntireMessage() throws Exception {
        SYM_ENCRYPT encrypt=create("defaultStore.keystore").encryptEntireMessage(true);
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

    protected static SYM_ENCRYPT create(String keystore) throws Exception {
        SYM_ENCRYPT encrypt=new SYM_ENCRYPT().keystoreName(keystore).encryptEntireMessage(false);
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
