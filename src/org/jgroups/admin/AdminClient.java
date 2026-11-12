package org.jgroups.admin;

import org.jgroups.Address;
import org.jgroups.Message;
import org.jgroups.ObjectMessage;
import org.jgroups.Version;
import org.jgroups.blocks.cs.Receiver;
import org.jgroups.blocks.cs.TcpClient;
import org.jgroups.stack.IpAddress;
import org.jgroups.util.*;

import javax.net.ssl.SNIHostName;
import java.io.DataInput;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;

/**
 * @author Bela Ban
 * @since x.y
 */
public class AdminClient implements Receiver {
    protected long       timeout=500;

    protected void start(InetAddress addr, int port, String command, long timeout, boolean udp, TLS tls) throws Exception {
        this.timeout=timeout;
        if(udp) {
            handleUdp(addr, port, command, timeout);
            return;
        }
        try(TcpClient client=new TcpClient(null, 0, addr, port)) {
            if(tls.enabled()) {
                tls.init();
                SocketFactory factory=tls.createSocketFactory();
                client.socketFactory(factory);
            }
            client.usePeerConnections(true).receiver(this);
            client.start();
            Address sender=client.localAddress();
            Message msg=new ObjectMessage(null, command).src(sender)
              .putHeader(Admin.ID, new AdminHeader(Admin.ADMIN_EXEC_KEY, AdminHeader.Type.request));
            ByteArrayDataOutputStream out=new ByteArrayDataOutputStream();
            Util.writeMessage(msg, out, true);
            ByteArray buf=out.getBuffer();
            client.send(null, buf.array(), 0, buf.length());
            Util.sleep(timeout);
        }
    }

    protected static void handleUdp(InetAddress addr, int port, String command, long timeout) throws Exception {
        try(DatagramSocket sock=new DatagramSocket(0, InetAddress.getLocalHost())) {
            sock.setSoTimeout((int)timeout);
            InetAddress local=sock.getLocalAddress();
            if(local == null)
                local=InetAddress.getLocalHost();
            Address sender=new IpAddress(local, sock.getLocalPort());
            Message msg=new ObjectMessage(null, command).src(sender)
              .putHeader(Admin.ID, new AdminHeader(Admin.ADMIN_EXEC_KEY, AdminHeader.Type.request));

            ByteArrayDataOutputStream out=new ByteArrayDataOutputStream();
            Util.writeMessage(msg, out, true);
            ByteArray buf=out.getBuffer();
            DatagramPacket p=new DatagramPacket(buf.array(), buf.offset(), buf.getLength(), addr, port);
            sock.send(p);

            byte[] b=new byte[16000];
            p=new DatagramPacket(b, 0, b.length);
            sock.receive(p);
            ByteArrayDataInputStream in=new ByteArrayDataInputStream(p.getData(), p.getOffset(), p.getLength());
            short ver=in.readShort();
            byte flags=in.readByte();
            System.out.printf("version: %s (flags: %d)\n", Version.print(ver), flags);
            msg=Util.readMessage(in);
            System.out.printf("-- rsp from %s:\n%s\n", msg.src(), msg.getObject());
        }
    }

    public static void main(String[] args) throws Exception {
        InetAddress host=InetAddress.getLocalHost();
        int         port=7800;
        String      command="";
        long        timeout=500;
        boolean     udp=false;
        TLS         tls=new TLS().enabled(false).setKeystorePath("keystore.jks").setKeystorePassword("password")
          .setClientAuth(TLSClientAuth.NEED).setKeystoreAlias("server"); // replace with actual values

        for(int i=0; i < args.length; i++) {
            String arg=args[i].trim();
            if(!arg.startsWith("-")) {
                command=String.format("%s %s", command, arg).strip();
                continue;
            }
            switch(arg) {
                case "-host":
                    host=InetAddress.getByName(args[++i]);
                    break;
                case "-port":
                    port=Integer.parseInt(args[++i]);
                    break;
                case "-timeout":
                    timeout=Long.parseLong(args[++i]);
                    break;
                case "-udp":
                    udp=true;
                    break;
                case "-tls":
                    tls.enabled(Boolean.parseBoolean(args[++i]));
                    break;
                case "-tls_protocol":
                    tls.enabled(true).setProtocols(args[++i].split(","));
                    break;
                case "-tls_cipher_suites":
                    tls.enabled(true).setCipherSuites(args[++i].split(","));
                    break;
                case "-tls_provider":
                    tls.enabled(true).setProvider(args[++i]);
                    break;
                case "-tls_keystore_path":
                    tls.enabled(true).setKeystorePath(args[++i]);
                    break;
                case "-tls_keystore_password":
                    tls.enabled(true).setKeystorePassword(args[++i]);
                    break;
                case "-tls_keystore_type":
                    tls.enabled(true).setKeystoreType(args[++i]);
                    break;
                case "-tls_keystore_alias":
                    tls.enabled(true).setKeystoreAlias(args[++i]);
                    break;
                case "-tls_truststore_path":
                    tls.enabled(true).setTruststorePath(args[++i]);
                    break;
                case "-tls_truststore_password":
                    tls.enabled(true).setTruststorePassword(args[++i]);
                    break;
                case "-tls_truststore_type":
                    tls.enabled(true).setTruststoreType(args[++i]);
                    break;
                case "-tls_client_auth":
                    tls.enabled(true).setClientAuth(TLSClientAuth.valueOf(args[++i].toUpperCase()));
                    break;
                case "-tls_sni_matcher":
                    tls.enabled(true).getSniMatchers().add(SNIHostName.createSNIMatcher(args[++i]));
                    break;
                default:
                    help();
                    return;
            }
        }
        if(command.isBlank()) {
            System.err.println("no command given");
            return;
        }
        /*tls.enabled(true).setKeystoreType("pkcs12").setKeystorePassword("password")
                .setKeystorePath("keystore.jks").setClientAuth(TLSClientAuth.NEED)
                  .setKeystoreAlias("server");
              tls.init();*/


        new AdminClient().start(host, port, command, timeout, udp, tls);
    }

    protected static void help() {
        System.out.printf("""
                            
                            %s [options] <command>
                                Options:
                                --------
                                -host host                   - The host to connect to
                                -port port                   - The port to connect to
                                -timeout millis              - The number of milliseconds to wait for a response
                                -udp                         - Needs to be set when connecting to a UDP transport
                                -tls <true | false>          - Enables or disables TLS (default: disabled)
                                -tls_protocol <protos>       - One or more TLS protocol names to use, e.g. TLSv1.2."
                                                               Setting this requires configuring key and trust stores.
                                -tls_provider <name>         - The name of the security provider to use for TLS.
                                -tls_keystore_path <file>    - The keystore path which contains the key.
                                -tls_keystore_password <password> - The key store password.
                                -tls_keystore_type <type>    - The type of keystore.
                                -tls_keystore_alias <alias>  - The alias of the key to use as identity for this Gossip router.
                                -tls_truststore_path <file>  - The truststore path.
                                -tls_truststore_password <password> - The trust store password.
                                -tls_truststore_type <type>  - The truststore type.
                                -tls_sni_matcher <name>      - A regular expression that servers use to match and accept SNI host names.
                                                               Can be repeated multiple times.
                                -tls_client_auth <mode>      - none (default), want or need. Whether to require client
                                                               certificate authentication.
                            """, AdminClient.class.getSimpleName());


    }

    @Override
    public void receive(Address sender, byte[] buf, int offset, int length) {
        try {
            receive(sender, new ByteArrayDataInputStream(buf, offset, length), length);
        }
        catch(Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void receive(Address sender, DataInput in, int length) throws Exception {
        short ver=in.readShort();
        if(!Version.isBinaryCompatible(ver))
            throw new IllegalStateException(String.format("version incompat: %d", ver));
        byte ignored=in.readByte();
        Message msg=Util.readMessage(in);
        System.out.printf("%s\n", (String)msg.getObject());
    }

}
