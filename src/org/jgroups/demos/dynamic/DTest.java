package org.jgroups.demos.dynamic;

import org.jgroups.*;
import org.jgroups.conf.ClassConfigurator;
import org.jgroups.util.Util;

import java.io.IOException;
import java.util.*;

/**
 * Dynamic test which can be started on several nodes. The test uses commands to dynamically invoke
 * functionality, such as a perf test, or reconfiguration of the channel.
 * @author Bela Ban
 * @since 3.1
 */
public class DTest extends ReceiverAdapter {
    public static final String DEFAULT_COMMANDS_PACKAGE="org.jgroups.demos.dynamic.commands";
    public static final short ID;

    static {
        ID=ClassConfigurator.getProtocolId(DTest.class);
    }

    protected JChannel ch;

    /** A hashmap of the registered commands */
    protected final Map<String,Command> commands=new HashMap<String,Command>();

    public JChannel getChannel() {
        return ch;
    }

    public void setChannel(JChannel ch) {
        if(ch != null) {
            this.ch=ch;
            this.ch.setReceiver(this);
        }
    }

    public void start(String props) throws Exception {
        ch=new JChannel(props);
        for(Command cmd: commands.values())
            cmd.setTest(this);
        ch.setReceiver(this);
        ch.connect("dtest-cluster");
    }

    protected void loop() throws IOException {
        for(;;) {
            System.out.print("> ");
            String line=Util.readLine(System.in);
            List<String> list=Util.parseStringList(line, " \t\n\r\f");

            String command_name=!list.isEmpty()? list.remove(0) : null;
            if(command_name == null)
                continue;

            List<Object> tmp=new ArrayList<Object>(list.size());
            tmp.addAll(list);

            Arguments args=new Arguments(command_name, tmp);

            // Process built-in commands first
            if(args.getCommandName().startsWith("exit"))
                break;
            if(args.getCommandName().startsWith("help")) {
                help();
                continue;
            }

            Command command=commands.get(args.getCommandName());
            if(command == null) {
                System.err.println("Command " + args.getCommandName() + " not found");
                continue;
            }

            try {
                command.invoke(args.getArguments());
            }
            catch(Exception e) {
                e.printStackTrace();
            }
        }
        Util.close(ch);
    }

    public void receive(Message msg) {
        DTestHeader hdr=(DTestHeader)msg.getHeader(ID);
        switch(hdr.type) {
            case DTestHeader.REQ:
                Arguments args=(Arguments)msg.getObject();
                Command command=commands.get(args.getCommandName());
                if(command == null)
                    System.err.println("Command " + args.getCommandName() + " not found");
                else {
                    Object rsp;
                    try {
                        rsp=command.invoke(args.getArguments());
                    }
                    catch(Exception e) {
                        e.printStackTrace();
                        rsp=e;
                    }
                    Message response=new Message(msg.getSrc(), null, rsp);
                    response.putHeader(ID, new DTestHeader(DTestHeader.RSP));
                    try {
                        ch.send(response);
                    }
                    catch(Exception e) {
                        e.printStackTrace();
                    }
                }
                break;
            case DTestHeader.RSP:
                Object rsp=msg.getObject();
                System.out.println(" response from " + msg.getSrc() + ": " + rsp);
                break;
            default:
                System.err.println("received an invalid header " + hdr);
                break;
        }
    }

    public void viewAccepted(View view) {
        System.out.println("-- view: " + view);
        System.out.print("> ");
    }

    public void help() {
        System.out.println("DTest [-props <props>] [-commands_package <package name>]");
        System.out.println("help exit");
        System.out.println("Commands:");
        for(Map.Entry<String,Command> entry: commands.entrySet())
            System.out.println(entry.getKey() + ": " + entry.getValue().help());
    }

    protected void findAndRegisterCommands(Collection<String> packages) throws Exception {
        for(String package_name: packages) {
            Set<Class<Command>> classes=Util.findClassesAssignableFrom(package_name,Command.class);
            for(Class<Command> clazz: classes) {
                String name=clazz.getSimpleName();
                Command command=clazz.newInstance();
                if(commands.containsKey(name))
                    throw new IllegalStateException("command " + name + " already exists");
                commands.put(name, command);
            }
        }
    }


    public static void main(String[] args) throws Exception {
        String             props=null;
        Collection<String> packages=new HashSet<String>();
        DTest test=new DTest();
        boolean print_help=false;

        packages.add(DEFAULT_COMMANDS_PACKAGE);
        for(int i=0; i < args.length; i++) {
            if(args[i].equals("-props")) {
                props=args[++i];
                continue;
            }
            if(args[i].equals("-commmands_package")) {
                String tmp_package=args[++i];
                packages.add(tmp_package);
            }
            print_help=true;
            break;
        }

        test.findAndRegisterCommands(packages);
        if(print_help) {
            test.help();
            return;
        }

        test.start(props);
        test.loop();
    }




}
