package org.jgroups.demos;

import org.jgroups.JChannel;
import org.jgroups.Message;
import org.jgroups.Receiver;
import org.jgroups.View;
import org.jgroups.logging.LogFactory;
import org.jgroups.protocols.*;
import org.jgroups.protocols.pbcast.GMS;
import org.jgroups.protocols.pbcast.NAKACK2;
import org.jgroups.protocols.pbcast.STABLE;
import org.jgroups.stack.*;
import org.jgroups.util.Util;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.Collections;
import java.util.Set;
import java.util.stream.Stream;

/**
 * @author Bela Ban
 */
public class ProgrammaticChat {
    protected static final JChannel ch;
    protected static final NonReflectiveProbeHandler h;

    static {

        try {
            LogFactory.useJdkLogger(true);
            // prevents setting default values: GraalVM doesn't accept creation of InetAddresses at build time (in the
            // image), so we have to set the default valiues at run time
            Configurator.skipSettingDefaultValues(true);

            Protocol[] prot_stack={
              new TCP().setBindPort(7800)
                .setDiagnosticsEnabled(true).diagEnableTcp(true).diagEnableUdp(false),
              new TCPPING(),
              new MPING(),
              new MERGE3(),
              new FD_SOCK(),
              new FD_ALL3(),
              new VERIFY_SUSPECT(),
              new NAKACK2(),
              new UNICAST3(),
              new STABLE(),
              new GMS().setJoinTimeout(1000),
              new UFC(),
              new MFC(),
              new FRAG2()
            };
            ch=new JChannel(prot_stack);

            Configurator.skipSettingDefaultValues(false);
            h=new NonReflectiveProbeHandler(ch).initialize(ch.getProtocolStack().getProtocols());

            ch.setReceiver(new Receiver() {
                public void viewAccepted(View new_view) {
                    System.out.println("view: " + new_view);
                }

                public void receive(Message msg) {
                    System.out.println("<< " + msg.getObject() + " [" + msg.getSrc() + "]");
                }
            });
        }
        catch(Exception ex) {
            throw new RuntimeException(ex);
        }
    }


    public static void main(String[] args) throws Exception {
        String name=null, bind_addr=null;
        for(int i=0; i < args.length; i++) {
            if("-name".equals(args[i])) {
                name=args[++i];
                continue;
            }
            if("-bind_addr".equals(args[i])) {
                bind_addr=args[++i];
                continue;
            }
            System.out.printf("%s [-h] [-name name] [-bind_addr addr]\n", ProgrammaticChat.class.getSimpleName());
            return;
        }

        ch.name(name);
        // Sets the default addresses in all protocols
        // Configurator.setDefaultAddressValues(ch.getProtocolStack().getProtocols(), Util.getIpStackType());

        InetAddress ba=bind_addr == null? Util.getAddress("site_local", Util.getIpStackType())
          : InetAddress.getByName(bind_addr);
        InetAddress diag_addr=Util.getAddress("224.0.75.75", Util.getIpStackType());
        InetAddress mping_mcast=Util.getAddress("230.5.6.7", Util.getIpStackType());
        ProtocolStack stack=ch.getProtocolStack();
        TP transport=stack.getTransport();
        transport.setBindAddress(ba).setDiagnosticsAddr(diag_addr);

        Discovery discovery=stack.findProtocol(TCPPING.class);
        if(discovery != null)
            ((TCPPING)discovery).initialHosts(Collections.singletonList(new InetSocketAddress(ba, 7800)));
        discovery=stack.findProtocol(MPING.class);
        if(discovery != null)
            ((MPING)discovery).setMcastAddr(mping_mcast);

        ch.connect("ChatCluster");

        DiagnosticsHandler diag_handler=transport.getDiagnosticsHandler();
        if(diag_handler != null) {
            Set<DiagnosticsHandler.ProbeHandler> probe_handlers=diag_handler.getProbeHandlers();
            probe_handlers.removeIf(probe_handler -> {
                String[] keys=probe_handler.supportedKeys();
                return keys != null && Stream.of(keys).anyMatch(s -> s.startsWith("jmx"));
            });
        }
        transport.registerProbeHandler(h);

        for(;;) {
            String line=Util.readStringFromStdin(": ");
            ch.send(null, line);
        }
    }

}


