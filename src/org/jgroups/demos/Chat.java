package org.jgroups.demos;

import org.jgroups.*;
import org.jgroups.admin.*;
import org.jgroups.util.Util;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

public class Chat implements Receiver {
    protected JChannel channel;
    protected static final String CLUSTER="chat";

    public void viewAccepted(View new_view) {
        System.out.println("** view: " + new_view);
    }

    public void receive(Message msg) {
        String line="[" + msg.getSrc() + "]: " + msg.getObject();
        System.out.println(line);
    }

    /** Method called from other app, injecting channel */
    public void start(JChannel ch) throws Exception {
        channel=ch;
        channel.setReceiver(this);
        channel.connect(CLUSTER);
        eventLoop();
        channel.close();
    }

    private void start(String props, String name, boolean nohup) throws Exception {
        try {
            channel=new JChannel(props).name(name);
            channel.setReceiver(this);
            channel.connect(CLUSTER);
        }
        catch(Exception ex) {
            Util.close(channel);
            throw ex;
        }

        if(!nohup) {
            eventLoop();
            channel.close();
        }
    }


    private void eventLoop() {
        channel.admin().register(PartitionHandler.KEY, new PartitionHandler(channel));
        BufferedReader in=new BufferedReader(new InputStreamReader(System.in));
        while(true) {
            try {
                System.out.print("> "); System.out.flush();
                String line=in.readLine();
                if(line == null)
                    continue;
                if(line.toLowerCase().startsWith("/quit") || line.toLowerCase().startsWith("/exit"))
                    break;

                if(line.startsWith("/handlers")) {
                    System.out.printf("handlers:\n%s\n", channel.admin().print());
                    continue;
                }

                if(line.startsWith("/probe")) {
                    String cmd=line.substring("/probe".length()).trim();
                    ProbeAdminHandler h=channel.admin().get("probe");
                    h.sendProbeRequest(null, cmd);
                    continue;
                }

                if(line.startsWith("/switch")) {
                    String new_config=line.substring("/switch".length()).trim();
                    SwitchAdminHandler h=channel.admin().get("switch");
                    h.listener((p, m) -> System.out.printf("-- [switch / %.2f %% done]: %s\n", p, m));
                    long start=System.nanoTime();
                    boolean result=h.switchToNewConfiguration(new_config);
                    long time=System.nanoTime()-start;
                    System.out.printf("-- result of attempting to switch to %s: %b (%s)\n",
                                      new_config, result, Util.printTime(time));
                    h.listener(null);
                    continue;
                }
                if(line.startsWith("/inject")) {
                    String partitions=line.substring("/inject".length()).trim();
                    PartitionHandler h=channel.admin().get(PartitionHandler.KEY);
                    h.injectView(partitions);
                    continue;
                }
                if(line.startsWith("/install-handler")) {
                    String s=line.substring("/install-handler".length()).trim();
                    Message msg=new ObjectMessage(null, String.format("%s %s", Admin.INSTALL_HANDLER, s))
                      .putHeader(Admin.ID, new AdminHeader(Admin.ADMIN_KEY, AdminHeader.Type.request));
                    channel.send(msg);
                    continue;
                }
                if(line.startsWith("/remove-handler")) {
                    String s=line.substring("/remove-handler".length()).trim();
                    Message msg=new ObjectMessage(null, String.format("%s %s", Admin.REMOVE_HANDLER, s))
                      .putHeader(Admin.ID, new AdminHeader(Admin.ADMIN_KEY, AdminHeader.Type.request));
                    channel.send(msg);
                    continue;
                }
                Message msg=new ObjectMessage(null, line);
                channel.send(msg);
            }
            catch(IOException | IllegalArgumentException io_ex) {
                break;
            }
            catch(Exception ex) {
                ex.printStackTrace();
            }
        }
    }


    public static void main(String[] args) throws Exception {
        String  props="udp.xml";
        String  name=null;
        boolean nohup=false;

        for(int i=0; i < args.length; i++) {
            if(args[i].equals("-props")) {
                props=args[++i];
                continue;
            }
            if(args[i].equals("-name")) {
                name=args[++i];
                continue;
            }
            if(args[i].equals("-nohup")) {
                nohup=true;
                continue;
            }
            help();
            return;
        }

        Chat ch=new Chat();
        ch.start(props, name, nohup);
    }

    protected static void help() {
        System.out.println("Chat [-props XML config] [-name name] [-nohup]");
    }


}
