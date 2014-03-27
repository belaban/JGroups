package org.jgroups.protocols;


import org.jgroups.Global;
import org.jgroups.auth.MD5Token;
import org.jgroups.auth.SimpleToken;
import org.testng.annotations.Test;

/**
 * A set of JUnit tests for the AUTH protocol
 * @author Chris Mills
 */
@Test(groups=Global.FUNCTIONAL,sequential=false)
public class AUTHTest {

    /**
     * Creates two SimpleToken objects with identical auth_values and authenticates one against the other
     * Test fails if an exception is thrown or authentication fails
     */
    public static void testSimpleToken() {
        SimpleToken token1=new SimpleToken();
        token1.setAuthValue("chris");
        SimpleToken token2=new SimpleToken();
        token2.setAuthValue("chris");
        assert token1.authenticate(token2, null);
    }

    /**
     * Creates two SimpleToken objects with different auth_values and authenticates one against the other
     * <p/>
     * Test fails if an exception is thrown or authentication passes
     */
    public static void testSimpleTokenMismatch() {
        SimpleToken token1=new SimpleToken();
        token1.setAuthValue("chris");
        SimpleToken token2=new SimpleToken();
        token2.setAuthValue("chrismills");
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
        MD5Token token1=new MD5Token();
        token1.setAuthValue("chris");
        token1.setHashType("MD5");

        MD5Token token2=new MD5Token();
        token2.setAuthValue("chris");
        token2.setHashType("MD5");

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
        MD5Token token1=new MD5Token();
        token1.setAuthValue("chris");
        token1.setHashType("MD5");

        MD5Token token2=new MD5Token();
        token2.setAuthValue("chrismills");
        token2.setHashType("MD5");

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
        MD5Token token1=new MD5Token();
        token1.setAuthValue("chris");
        token1.setHashType("SHA");

        MD5Token token2=new MD5Token();
        token2.setAuthValue("chris");
        token2.setHashType("SHA");

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
        MD5Token token1=new MD5Token();
        token1.setAuthValue("chris");
        token1.setHashType("SHA");

        MD5Token token2=new MD5Token();
        token2.setAuthValue("chrismills");
        token2.setHashType("SHA");

        assert !token1.authenticate(token2, null);
    }

    /**
     * Test to create an AuthHeader object and set and get the Token object
     * <p/>
     * Fails if an exception is thrown or the set and get don't equal the same object
     */
    public static void testAuthHeader() {
        SimpleToken token1=new SimpleToken();
        token1.setAuthValue("chris");

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
        SimpleToken token1=new SimpleToken();
        token1.setAuthValue("chris");

        SimpleToken token2=new SimpleToken();
        token2.setAuthValue("chris");

        AuthHeader header=new AuthHeader();
        header.setToken(token1);

        assert !(token2 == header.getToken());
    }

    public void testAuth() {
       
       
    }

}
