package org.jgroups.protocols;

import javax.net.ssl.SSLPeerUnverifiedException;
import javax.net.ssl.SSLSession;
import java.security.Principal;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Sample implementation of {@link org.jgroups.protocols.SSL_KEY_EXCHANGE.SessionVerifier}
 * @author Bela Ban
 * @since  4.0.6
 */
public class CertficateCNMatcher implements SSL_KEY_EXCHANGE.SessionVerifier {
    protected String  cn_name;
    protected Pattern pattern;

    public void init(String arg) {
        cn_name=arg;
        pattern=Pattern.compile(cn_name);
    }

    public void verify(SSLSession session) throws SecurityException {
        Principal principal=null;
        try {
            principal=session.getPeerPrincipal();
            String name=principal.getName();
            Matcher m=pattern.matcher(name);
            boolean find=m.find();
            if(!find)
                throw new SecurityException(String.format("pattern '%s' not found in peer certificate '%s'",
                                                          cn_name, name));
            else
                System.out.printf("** pattern '%s' found in peer certificate '%s'\n",
                                  cn_name, name);
        }
        catch(SSLPeerUnverifiedException e) {
            throw new SecurityException(e);
        }
    }
}
