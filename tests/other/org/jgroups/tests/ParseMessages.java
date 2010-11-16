package org.jgroups.tests;

import org.jgroups.Address;
import org.jgroups.Message;
import org.jgroups.Version;
import org.jgroups.util.Util;

import java.io.*;
import java.util.LinkedList;
import java.util.List;

/**
 * Parses messages out of a captured file and writes them to stdout
 * @author Bela Ban
 */
public class ParseMessages {
    InputStream input=null;
    private static final byte LIST=1; // we have a list of messages rather than a single message when set
    private static final byte MULTICAST=2; // message is a multicast (versus a unicast) message when set
    
    public ParseMessages(String input) throws FileNotFoundException {
        this.input=new FileInputStream(input);
    }

    public void parse() {
        short           version;
        byte            flags;
        DataInputStream dis=null;

        try {
            dis=new DataInputStream(input);

            for(;;) {
                version=dis.readShort();

//                int ch1 = input.read();
//                int ch2 = input.read();
//                if ((ch1 | ch2) < 0)
//                    throw new EOFException();
//                version=(short)((ch1 << 8) + (ch2 << 0));


                System.out.println("version = " + version + " (" + Version.print(version) + ")");
                flags=dis.readByte();
                System.out.println("flags: " + Message.flagsToString(flags));

                boolean is_message_list=(flags & LIST) == LIST;
                boolean multicast=(flags & MULTICAST) == MULTICAST;

                if(is_message_list) { // used if message bundling is enabled
                    List<Message> msgs=readMessageList(dis);
                    System.out.println(msgs.size() + " msgs: ");
                    int cnt=1;
                    for(Message msg: msgs) {
                        System.out.print("#" + cnt++ + ": ");
                        print(msg, multicast);
                    }
                }
                else {
                    Message msg=readMessage(dis);
                    print(msg, multicast);
                }
            }
        }
        catch(Throwable t) {
            t.printStackTrace();
        }
        finally {
            Util.close(dis);
        }
    }

    private static void print(Message msg, boolean multicast) {
        System.out.println(msg + ", hdrs: " + msg.printHeaders() + ", mcast: " + multicast);
    }

    private static List<Message> readMessageList(DataInputStream in) throws Exception {
        List<Message> list=new LinkedList<Message>();
        Address dest=Util.readAddress(in);
        Address src=Util.readAddress(in);

        while(in.readBoolean()) {
            Message msg=new Message(false);
            msg.readFrom(in);
            msg.setDest(dest);
            if(msg.getSrc() == null)
                msg.setSrc(src);
            list.add(msg);
        }
        return list;
    }

    protected static Message readMessage(DataInputStream instream) throws Exception {
        Message msg=new Message(false); // don't create headers, readFrom() will do this
        msg.readFrom(instream);
        return msg;
    }


    public static void main(String[] args) throws FileNotFoundException {
        String file=null;

        for(int i=0; i < args.length; i++) {
            if(args[i].equals("-file")) {
                file=args[++i];
                continue;
            }
            help();
            return;
        }

        new ParseMessages(file).parse();
    }

    static private void help() {
        System.out.println("ParseMessages [-file <filename>]");
    }
}
