/*
 * Created on 04-Jul-2004
 */
package org.jgroups.protocols;


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
@Test(groups=Global.FUNCTIONAL)
public class ENCRYPTKeystoreTest {

    static final short ENCRYPT_ID=ClassConfigurator.getProtocolId(SYM_ENCRYPT.class);

    protected String symAlgorithm() { return "AES"; }
    protected int symIvLength() { return 0; }

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

        encrypt.down(msg);
        Message sentMsg=observer.getDownMessages().get("message0");
        String encText=new String(sentMsg.getBuffer());
        assert !encText.equals(messageText);
        EncryptHeader encHeader = sentMsg.getHeader(ENCRYPT_ID);
        assert encHeader != null;
        byte[] decodedBytes=encrypt.code(sentMsg.getRawBuffer(), sentMsg.getOffset(), sentMsg.getLength(), encHeader.iv(), true);
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
        byte[] iv = encrypt2.makeIv();
        byte[] encodedBytes=encrypt2.code(bytes, 0, bytes.length, iv, false);
        assert !new String(encodedBytes).equals(messageText);

        MessageDigest digest=MessageDigest.getInstance("MD5");
        digest.reset();
        digest.update(encrypt.secretKey().getEncoded());

        byte[] symVersion=digest.digest();
        Message msg=new Message(null, encodedBytes).putHeader(ENCRYPT_ID, new EncryptHeader((byte)0, symVersion, iv));
        encrypt.up(msg);
        Message rcvdMsg=observer.getUpMessages().get("message0");
        String decText=new String(rcvdMsg.getBuffer());
        assert decText.equals(messageText);

    }

    public void testMessageUpWrongKey() throws Exception {
        SYM_ENCRYPT encrypt=create("defaultStore.keystore"), encrypt2=create("defaultStore2.keystore");
        MockProtocol observer=new MockProtocol();
        encrypt.setUpProtocol(observer);

        String messageText="hello this is a test message";
        byte[] bytes=messageText.getBytes();
        byte[] iv = encrypt2.makeIv();
        byte[] encodedBytes=encrypt2.code(bytes, 0, bytes.length, iv, false);
        assert !new String(encodedBytes).equals(messageText);

        MessageDigest digest=MessageDigest.getInstance("MD5");
        digest.reset();
        digest.update(encrypt2.secretKey().getEncoded());

        byte[] symVersion=digest.digest();

        Message msg=new Message(null, encodedBytes).putHeader(ENCRYPT_ID, new EncryptHeader((byte)0, symVersion, iv));
        encrypt.up(msg);
        assert observer.getUpMessages().isEmpty();
    }

    public void testMessageUpNoEncryptHeader() throws Exception {
        SYM_ENCRYPT encrypt=create("defaultStore.keystore"), encrypt2=create("defaultStore.keystore");
        MockProtocol observer=new MockProtocol();
        encrypt.setUpProtocol(observer);
        String messageText="hello this is a test message";
        byte[] bytes=messageText.getBytes();
        byte[] encodedBytes=encrypt2.code(bytes, 0, bytes.length, encrypt2.makeIv(), false);
        assert !new String(encodedBytes).equals(messageText);
        Message msg=new Message(null, encodedBytes);
        encrypt.up(msg);
        assert observer.getUpMessages().isEmpty();
    }


    public void testMessageUpNoBuffer() throws Exception {
        SYM_ENCRYPT encrypt=create("defaultStore.keystore");
        MockProtocol observer=new MockProtocol();
        encrypt.setUpProtocol(observer);
        encrypt.up(new Message().putHeader(ENCRYPT_ID, new EncryptHeader((byte)0, "bla".getBytes(), encrypt.makeIv())));
        assert observer.getUpMessages().isEmpty();
    }

    public void testEncryptEntireMessage() throws Exception {
        SYM_ENCRYPT encrypt=create("defaultStore.keystore");
        Message msg=new Message(null, "hello world".getBytes()).putHeader((short)1, new TpHeader("cluster"));
        MockProtocol mock=new MockProtocol();
        encrypt.setDownProtocol(mock);
        encrypt.down(msg);

        Message encrypted_msg=mock.getDownMessages().get("message0");

        encrypt.setDownProtocol(null);
        encrypt.setUpProtocol(mock);
        encrypt.up(encrypted_msg);

        Message decrypted_msg=mock.getUpMessages().get("message1");
        String temp=new String(decrypted_msg.getBuffer());
        assert "hello world".equals(temp);
    }

    protected SYM_ENCRYPT create(String keystore) throws Exception {
        SYM_ENCRYPT encrypt=new SYM_ENCRYPT().keystoreName(keystore).symAlgorithm(symAlgorithm()).symIvLength(symIvLength());
        encrypt.init();
        return encrypt;
    }


    protected static class MockProtocol extends Protocol {
        private final Map<String,Message> upMessages=new HashMap<>();
        private final Map<String,Message> downMessages=new HashMap<>();
        private int                       counter;

        public Map<String,Message> getDownMessages() {return downMessages;}
        public Map<String,Message> getUpMessages()   {return upMessages;}

        public Object down(Message msg) {
            downMessages.put("message" + counter++, msg);
            return null;
        }

        public Object up(Message msg) {
            upMessages.put("message" + counter++, msg);
            return null;
        }

        public void up(MessageBatch batch) {throw new UnsupportedOperationException();}
    }



}
