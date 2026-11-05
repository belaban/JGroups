package org.jgroups.admin;

import org.jgroups.Address;
import org.jgroups.Message;
import org.jgroups.ObjectMessage;
import org.jgroups.Version;
import org.jgroups.blocks.cs.Receiver;
import org.jgroups.blocks.cs.TcpClient;
import org.jgroups.util.ByteArray;
import org.jgroups.util.ByteArrayDataInputStream;
import org.jgroups.util.ByteArrayDataOutputStream;
import org.jgroups.util.Util;

import java.io.DataInput;
import java.net.InetAddress;

/**
 * @author Bela Ban
 * @since x.y
 */
public class Client implements Receiver {
    protected long timeout=500;

    protected void start(InetAddress addr, int port, String command, long timeout) throws Exception {
        this.timeout=timeout;
        try(TcpClient client=new TcpClient(null, 0, addr, port)) {
            client.usePeerConnections(true).receiver(this);
            client.start();
            Address sender=client.localAddress();
            Message msg=new ObjectMessage(null, command).src(sender)
              .putHeader(Admin.ID, new AdminHeader(Admin.ADMIN_EXEC_KEY, AdminHeader.Type.request));
            ByteArrayDataOutputStream out=new ByteArrayDataOutputStream();
            Util.writeMessage(msg, out, true);
            ByteArray buf=out.getBuffer();
            client.send(null, buf.array(), 0, buf.length());
            Util.sleep(500);
        }
    }

    public static void main(String[] args) throws Exception {
        InetAddress host=InetAddress.getLocalHost();
        int         port=7800;
        String      command="";
        long        timeout=500;

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
                default:
                    help();
                    return;
            }
        }
        if(command.isBlank()) {
            System.err.println("no command given");
            return;
        }
        new Client().start(host, port, command, timeout);
    }

    protected static void help() {
        System.out.printf("%s [-host host] [-port port] [-timeout millis] command\n", Client.class.getName());
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
