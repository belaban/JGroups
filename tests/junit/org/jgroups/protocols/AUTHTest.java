package org.jgroups.protocols;

import junit.framework.TestCase;
import junit.framework.Test;
import junit.framework.TestSuite;
import org.jgroups.auth.SimpleToken;
import org.jgroups.auth.AuthToken;
import org.jgroups.auth.MD5Token;

import java.util.Properties;

/**
 * A set of JUnit tests for the AUTH protocol
 *
 * @author Chris Mills
 */
public class AUTHTest extends TestCase{
    Properties properties = new Properties();

    /**
     * Creates two SimpleToken objects with identical auth_values and authenticates one against the other
     *
     * Test fails if an exception is thrown or authentication fails
     */
    public void testSimpleToken(){
        try{
            properties.clear();

            properties.put("auth_value", "chris");

            SimpleToken token1 = new SimpleToken();
            token1.setValue(properties);

            properties.put("auth_value", "chris");

            SimpleToken token2 = new SimpleToken();
            token2.setValue(properties);

            assertTrue(token1.authenticate(token2, null));
        }catch(Exception failException){
            fail(failException.getMessage());
        }
    }
    /**
     * Creates two SimpleToken objects with different auth_values and authenticates one against the other
     *
     * Test fails if an exception is thrown or authentication passes
     */
    public void testSimpleTokenMismatch(){
        try{
            properties.clear();

            properties.put("auth_value", "chris");

            SimpleToken token1 = new SimpleToken();
            token1.setValue(properties);

            properties.put("auth_value", "chrismills");

            SimpleToken token2 = new SimpleToken();
            token2.setValue(properties);

            assertTrue(!token1.authenticate(token2, null));
        }catch(Exception failException){
            fail(failException.getMessage());
        }
    }

    /**
     * Creates two MD5Token objects with identical auth_values and authenticates one against the other
     *
     * Uses an MD5 hash type
     *
     * Test fails if an exception is thrown or authentication fails
     */
    public void testMD5Token(){
        try{
            properties.clear();

            properties.put("auth_value", "chris");
            properties.put("token_hash", "MD5");

            MD5Token token1 = new MD5Token();
            token1.setValue(properties);

            properties.put("auth_value", "chris");
            properties.put("token_hash", "MD5");

            MD5Token token2 = new MD5Token();
            token2.setValue(properties);

            assertTrue(token1.authenticate(token2, null));
        }catch(Exception failException){
            fail(failException.getMessage());
        }
    }

    /**
     * Creates two MD5Token objects with different auth_values and authenticates one against the other
     *
     * Uses an MD5 hash type
     *
     * Test fails if an exception is thrown or authentication passes
     */
    public void testMD5TokenMismatch(){
        try{
            properties.clear();

            properties.put("auth_value", "chris");
            properties.put("token_hash", "MD5");

            MD5Token token1 = new MD5Token();
            token1.setValue(properties);

            properties.put("auth_value", "chrismills");
            properties.put("token_hash", "MD5");

            MD5Token token2 = new MD5Token();
            token2.setValue(properties);

            assertTrue(!token1.authenticate(token2, null));
        }catch(Exception failException){
            fail(failException.getMessage());
        }
    }

    /**
     * Creates two MD5Token objects with identical auth_values and authenticates one against the other
     *
     * Uses an SHA hash type
     *
     * Test fails if an exception is thrown or authentication fails
     */
    public void testSHAToken(){
        try{
            properties.clear();

            properties.put("auth_value", "chris");
            properties.put("token_hash", "SHA");

            MD5Token token1 = new MD5Token();
            token1.setValue(properties);

            properties.put("auth_value", "chris");
            properties.put("token_hash", "SHA");

            MD5Token token2 = new MD5Token();
            token2.setValue(properties);

            assertTrue(token1.authenticate(token2, null));
        }catch(Exception failException){
            fail(failException.getMessage());
        }
    }

    /**
     * Creates two MD5Token objects with different auth_values and authenticates one against the other
     *
     * Uses an SHA hash type
     *
     * Test fails if an exception is thrown or authentication passes
     */
    public void testSHATokenMismatch(){
        try{
            properties.clear();

            properties.put("auth_value", "chris");
            properties.put("token_hash", "SHA");

            MD5Token token1 = new MD5Token();
            token1.setValue(properties);

            properties.put("auth_value", "chrismills");
            properties.put("token_hash", "SHA");

            MD5Token token2 = new MD5Token();
            token2.setValue(properties);

            assertTrue(!token1.authenticate(token2, null));
        }catch(Exception failException){
            fail(failException.getMessage());
        }
    }

    /**
     * Test to create an AuthHeader object and set and get the Token object
     *
     * Fails if an exception is thrown or the set and get don't equal the same object
     */
    public void testAuthHeader(){
        try{
            properties.clear();

            properties.put("auth_value", "chris");

            SimpleToken token1 = new SimpleToken();
            token1.setValue(properties);

            AuthHeader header = new AuthHeader();
            header.setToken(token1);

            assertTrue(token1 == header.getToken());
        }catch(Exception failException){
            fail(failException.getMessage());
        }
    }

    /**
     * Test to create an AuthHeader object and set and get the Token object
     *
     * Fails if an exception is thrown or the set and get equal the same object
     */
    public void testAuthHeaderDifferent(){
        try{
            properties.clear();

            properties.put("auth_value", "chris");

            SimpleToken token1 = new SimpleToken();
            token1.setValue(properties);

            properties.put("auth_value", "chris");

            SimpleToken token2 = new SimpleToken();
            token2.setValue(properties);

            AuthHeader header = new AuthHeader();
            header.setToken(token1);

            assertTrue(!(token2 == header.getToken()));
        }catch(Exception failException){
            fail(failException.getMessage());
        }
    }


    public static Test suite() {
        TestSuite s = new TestSuite(AUTHTest.class);
        return s;
    }

    public static void main(String[] args) {
        junit.textui.TestRunner.run(suite());
    }

}
