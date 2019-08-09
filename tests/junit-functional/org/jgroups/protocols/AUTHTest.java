package org.jgroups.protocols;


import org.jgroups.Global;
import org.jgroups.JChannel;
import org.jgroups.auth.FixedMembershipToken;
import org.jgroups.auth.MD5Token;
import org.jgroups.auth.SimpleToken;
import org.jgroups.protocols.pbcast.GMS;
import org.jgroups.stack.IpAddress;
import org.jgroups.stack.ProtocolStack;
import org.jgroups.util.Util;
import org.testng.annotations.Test;

/**
 * A set of tests for the AUTH protocol
 * @author Chris Mills
 */
@Test(groups=Global.FUNCTIONAL)
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
    public void testMD5Token() {
        MD5Token token1=new MD5Token();
        token1.setAuthValue("chris");
        token1.setHashType("MD5");

        MD5Token token2=new MD5Token();
        token2.setAuthValue("chris");
        token2.setHashType("MD5");

        assert token1.authenticate(token2, null);

        token1=new MD5Token("chris", "MD5");
        token2=new MD5Token("chriss", "MD5");
        assert !token1.authenticate(token2, null);
        token2=new MD5Token("chris", "MD5");
        assert token2.authenticate(token1, null);
        assert !token1.getAuthValue().equalsIgnoreCase("chris");
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
    public void testAuthHeaderDifferent() {
        SimpleToken token1=new SimpleToken();
        token1.setAuthValue("chris");

        SimpleToken token2=new SimpleToken();
        token2.setAuthValue("chris");

        AuthHeader header=new AuthHeader();
        header.setToken(token1);

        assert !(token2 == header.getToken());
    }


    public void testFixedMembershipTokenIPv4() throws Exception {
        FixedMembershipToken tok=new FixedMembershipToken();
        tok.setMemberList("192.168.1.6,10.1.1.1/7500,localhost/7800");
        assert !tok.isInMembersList(new IpAddress("192.168.1.3", 7500));
        assert !tok.isInMembersList(new IpAddress("10.1.1.1", 7000));
        assert tok.isInMembersList(new IpAddress("10.1.1.1", 7500));
        assert tok.isInMembersList(new IpAddress("192.168.1.6", 7500)); // port is not matched
        assert tok.isInMembersList(new IpAddress("192.168.1.6", 0));    // port is not matched
    }


    public void testFixedMembershipTokenIPv6() throws Exception {
        FixedMembershipToken tok=new FixedMembershipToken();
        tok.setMemberList("fe80::aa20:66ff:fe11:d346,2a02:120b:2c45:1b70:aa20:66ff:fe11:d346/7500,2a02:120b:2c45:1b70:f474:e6ca:3038:6b5f/7500");
        assert tok.isInMembersList(new IpAddress("2a02:120b:2c45:1b70:f474:e6ca:3038:6b5f", 7500));
    }

    public void testJoinOfSecondNodeWithoutAuthHeader() throws Exception {
        JChannel a=create("A", true), b=create("B", false);
        a.connect("auth-test");

        try {
            b.connect("auth-test");
            assert false : "the second member's JOIN should have thrown an exception";
        }
        catch(Exception ex) {
            System.out.printf("received exception as expected: %s\n", ex.getCause());
        }
        finally {
            Util.close(b,a);
        }
    }

    protected static JChannel create(String name, boolean create_auth_prot) throws Exception {
        JChannel ch=new JChannel(Util.getTestStack()).name(name);
        if(create_auth_prot) {
            AUTH auth=new AUTH().setAuthToken(new SimpleToken("foo")); //.setAuthCoord(false)
            ch.getProtocolStack().insertProtocol(auth, ProtocolStack.Position.BELOW, GMS.class);
        }
        return ch;
    }

}
