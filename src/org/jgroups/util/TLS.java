package org.jgroups.util;

import org.jgroups.Lifecycle;
import org.jgroups.annotations.Property;
import org.jgroups.conf.PropertyConverter;

import javax.net.ssl.SNIMatcher;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLParameters;
import javax.net.ssl.SSLServerSocket;
import java.util.ArrayList;
import java.util.List;

/**
 * Component to configure TLS in protocols ({@link org.jgroups.protocols.TCP} and {@link org.jgroups.protocols.TUNNEL}).
 * @author Bela Ban
 * @since  5.2.15
 */
public class TLS implements Lifecycle {

    @Property(description="Enables TLS; when true, SSL sockets will be used instead of regular sockets")
    protected boolean          enabled;

    @Property(description="One or more TLS protocol names to use, e.g. TLSv1.2. Setting this requires " +
      "configuring key and trust stores")
    protected String[]         protocols={"TLSv1.2"};

    @Property(description="The list of cipher suites")
    protected String[]         cipher_suites;

    @Property(description="The security provider. Defaults to null, which will use the default JDK provider")
    protected String           provider;

    @Property(description="Fully qualified path to the keystore")
    protected String           keystore_path;

    @Property(description="Keystore password",exposeAsManagedAttribute=false)
    protected String           keystore_password;

    @Property(description="The type of the keystore")
    protected String           keystore_type="pkcs12";

    @Property(description="Alias used for fetching the key")
    protected String           keystore_alias;

    @Property(description="Fully qualified path to the truststore")
    protected String           truststore_path;

    @Property(description="The password of the truststore",exposeAsManagedAttribute=false)
    protected String           truststore_password;

    @Property(description="The type of the truststore")
    protected String           truststore_type="pkcs12";

    @Property(description="Defines whether client certificate authentication is required. " +
      "Legal values are NONE, WANT or NEED")
    protected TLSClientAuth    client_auth=TLSClientAuth.NONE;

    @Property(description="A list of regular expression that servers use to match and accept SNI host names",
      converter=SniMatcherConverter.class)
    protected List<SNIMatcher> sni_matchers=new ArrayList<>();




    public boolean enabled()                      {return enabled;}
    public TLS enabled(boolean e)                 {this.enabled=e; return this;}

    public String[] getProtocols()                {return protocols;}
    public TLS setProtocols(String[] p)           {this.protocols=p; return this;}

    public String[] getCipherSuites()             {return cipher_suites;}
    public TLS setCipherSuites(String[] c)        {this.cipher_suites=c; return this;}

    public String getProvider()                   {return provider;}
    public TLS setProvider(String p)              {this.provider=p; return this;}

    public String getKeystorePath()               {return keystore_path;}
    public TLS setKeystorePath(String k)          {this.keystore_path=k; return this;}

    public String getKeystorePassword()           {return keystore_password;}
    public TLS setKeystorePassword(String k)      {this.keystore_password=k; return this;}

    public String getKeystoreType()               {return keystore_type;}
    public TLS setKeystoreType(String k)          {this.keystore_type=k; return this;}

    public String getKeystoreAlias()              {return keystore_alias;}
    public TLS setKeystoreAlias(String k)         {this.keystore_alias=k; return this;}

    public String getTruststorePath()             {return truststore_path;}
    public TLS setTruststorePath(String t)        {this.truststore_path=t; return this;}

    public String getTruststorePassword()         {return truststore_password;}
    public TLS setTruststorePassword(String t)    {this.truststore_password=t; return this;}

    public String getTruststoreType()             {return truststore_type;}
    public TLS setTruststoreType(String t)        {this.truststore_type=t; return this;}

    public TLSClientAuth getClientAuth()          {return client_auth;}
    public TLS setClientAuth(TLSClientAuth c)     {this.client_auth=c; return this;}

    public List<SNIMatcher> getSniMatchers()      {return sni_matchers;}
    public TLS setSniMatchers(List<SNIMatcher> s) {this.sni_matchers=s; return this;}


    @Override
    public void init() throws Exception {
        if(truststore_path == null) {
            // Truststore not given, so use the keystore as a truststore for backwards compatibility
            truststore_path=keystore_path;
            truststore_type=keystore_type;
            truststore_password=keystore_password;
        }
    }

    public SSLContext createContext() {
        SslContextFactory sslContextFactory=new SslContextFactory();
        sslContextFactory
          .classLoader(this.getClass().getClassLoader())
          .sslProtocol("TLS")
          .provider(provider)
          .keyStoreFileName(keystore_path)
          .keyStorePassword(keystore_password)
          .keyStoreType(keystore_type)
          .keyAlias(keystore_alias)
          .trustStoreFileName(truststore_path)
          .trustStorePassword(truststore_password)
          .trustStoreType(truststore_type);
        return sslContextFactory.getContext();
    }

    public SocketFactory createSocketFactory() {
        SSLContext context=createContext();
        return createSocketFactory(context);
    }

    public SocketFactory createSocketFactory(SSLContext context) {
        DefaultSocketFactory socketFactory=new DefaultSocketFactory(context);
        final SSLParameters serverParameters=new SSLParameters();
        if(protocols != null)
            serverParameters.setProtocols(protocols);
        if(cipher_suites != null)
            serverParameters.setCipherSuites(cipher_suites);
        serverParameters.setSNIMatchers(sni_matchers);
        switch(client_auth) {
            case NEED:
                serverParameters.setNeedClientAuth(true);
                break;
            case WANT:
                serverParameters.setWantClientAuth(true);
                break;
            default:
                break;
        }
        socketFactory.setServerSocketConfigurator(s -> ((SSLServerSocket)s).setSSLParameters(serverParameters));
        return socketFactory;
    }


    public static class SniMatcherConverter implements PropertyConverter {
        @Override public Object convert(Object obj, Class<?> field_type, String name, String val,
                                        boolean check_scope, StackType ip_version) throws Exception {
            if(val == null)
                return null;
            List<String> list=Util.parseStringList(val, ",");
            List<SNIMatcher> retval=new ArrayList<>(list.size());
            for(String s: list) {
                Class<SNIMatcher> cl=(Class<SNIMatcher>)Util.loadClass(s, getClass());
                SNIMatcher m=cl.getConstructor().newInstance();
                retval.add(m);
            }
            return retval;
        }

        @Override
        public String toString(Object value) {
            return value != null? value.getClass().getSimpleName() : null;
        }
    }
}
