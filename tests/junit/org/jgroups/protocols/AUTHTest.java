package org.jgroups.protocols;


import org.jgroups.auth.MD5Token;
import org.jgroups.auth.SimpleToken;
import org.jgroups.tests.ChannelTestBase;
import org.testng.annotations.Test;

import java.util.Properties;

/**
 * A set of JUnit tests for the AUTH protocol
 * @author Chris Mills
 */
@Test(sequential=false)
public class AUTHTest extends ChannelTestBase {

    /**
     * Creates two SimpleToken objects with identical auth_values and authenticates one against the other
     * Test fails if an exception is thrown or authentication fails
     */
    public static void testSimpleToken() {
        Properties properties=new Properties();
        properties.put("auth_value", "chris");

        SimpleToken token1=new SimpleToken();
        token1.setValue(properties);

        properties.put("auth_value", "chris");

        SimpleToken token2=new SimpleToken();
        token2.setValue(properties);
        assert token1.authenticate(token2, null);
    }

    /**
     * Creates two SimpleToken objects with different auth_values and authenticates one against the other
     * <p/>
     * Test fails if an exception is thrown or authentication passes
     */
    public static void testSimpleTokenMismatch() {
        Properties properties=new Properties();
        properties.put("auth_value", "chris");

        SimpleToken token1=new SimpleToken();
        token1.setValue(properties);

        properties.put("auth_value", "chrismills");

        SimpleToken token2=new SimpleToken();
        token2.setValue(properties);

        assert !token1.authenticate(token2, null);
    }

    /**
     * Creates two MD5Token objects with identical auth_values and authenticates one against the other
     * <p/>
     * Uses an MD5 hash type
     * <p/>
     * Test fails if an exception is thrown or authentication fails
     */
    public static void testMD5Token() {
        Properties properties=new Properties();
        properties.put("auth_value", "chris");
        properties.put("token_hash", "MD5");

        MD5Token token1=new MD5Token();
        token1.setValue(properties);

        properties.put("auth_value", "chris");
        properties.put("token_hash", "MD5");

        MD5Token token2=new MD5Token();
        token2.setValue(properties);

        assert token1.authenticate(token2, null);
    }

    /**
     * Creates two MD5Token objects with different auth_values and authenticates one against the other
     * <p/>
     * Uses an MD5 hash type
     * <p/>
     * Test fails if an exception is thrown or authentication passes
     */
    public static void testMD5TokenMismatch() {
        Properties properties=new Properties();
        properties.put("auth_value", "chris");
        properties.put("token_hash", "MD5");

        MD5Token token1=new MD5Token();
        token1.setValue(properties);

        properties.put("auth_value", "chrismills");
        properties.put("token_hash", "MD5");

        MD5Token token2=new MD5Token();
        token2.setValue(properties);

        assert !token1.authenticate(token2, null);
    }

    /**
     * Creates two MD5Token objects with identical auth_values and authenticates one against the other
     * <p/>
     * Uses an SHA hash type
     * <p/>
     * Test fails if an exception is thrown or authentication fails
     */
    public static void testSHAToken() {
        Properties properties=new Properties();
        properties.put("auth_value", "chris");
        properties.put("token_hash", "SHA");

        MD5Token token1=new MD5Token();
        token1.setValue(properties);

        properties.put("auth_value", "chris");
        properties.put("token_hash", "SHA");

        MD5Token token2=new MD5Token();
        token2.setValue(properties);

        assert token1.authenticate(token2, null);
    }

    /**
     * Creates two MD5Token objects with different auth_values and authenticates one against the other
     * <p/>
     * Uses an SHA hash type
     * <p/>
     * Test fails if an exception is thrown or authentication passes
     */
    public static void testSHATokenMismatch() {
        Properties properties=new Properties();
        properties.put("auth_value", "chris");
        properties.put("token_hash", "SHA");

        MD5Token token1=new MD5Token();
        token1.setValue(properties);

        properties.put("auth_value", "chrismills");
        properties.put("token_hash", "SHA");

        MD5Token token2=new MD5Token();
        token2.setValue(properties);

        assert !token1.authenticate(token2, null);
    }

    /**
     * Test to create an AuthHeader object and set and get the Token object
     * <p/>
     * Fails if an exception is thrown or the set and get don't equal the same object
     */
    public static void testAuthHeader() {
        Properties properties=new Properties();
        properties.put("auth_value", "chris");

        SimpleToken token1=new SimpleToken();
        token1.setValue(properties);

        AuthHeader header=new AuthHeader();
        header.setToken(token1);

        assert token1 == header.getToken();
    }

    /**
     * Test to create an AuthHeader object and set and get the Token object
     * <p/>
     * Fails if an exception is thrown or the set and get equal the same object
     */
    public static void testAuthHeaderDifferent() {
        Properties properties=new Properties();
        properties.put("auth_value", "chris");

        SimpleToken token1=new SimpleToken();
        token1.setValue(properties);

        properties.put("auth_value", "chris");

        SimpleToken token2=new SimpleToken();
        token2.setValue(properties);

        AuthHeader header=new AuthHeader();
        header.setToken(token1);

        assert !(token2 == header.getToken());
    }


}
