package org.jgroups.tests;

import org.jgroups.Message;
import org.jgroups.Version;
import org.jgroups.protocols.TP;
import org.jgroups.util.MessageBatch;
import org.jgroups.util.Util;

import java.io.*;
import java.util.ArrayList;
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

    public ParseMessages(InputStream input) {
        this.input=input;
    }

    public List<Message> parse() {
        List<Message>   retval=new ArrayList<>();
        DataInputStream dis=null;
        try {
            dis=new DataInputStream(input);

            short version;
            for(;;) {
                try {
                    version=dis.readShort();
                }
                catch(IOException io_ex) {
                    break;
                }

                System.out.println("version = " + version + " (" + Version.print(version) + ")");
                byte flags=dis.readByte();
                System.out.println("flags: " + Message.flagsToString(flags));

                boolean is_message_list=(flags & LIST) == LIST;
                boolean multicast=(flags & MULTICAST) == MULTICAST;

                if(is_message_list) { // used if message bundling is enabled
                    final MessageBatch[] batches=TP.readMessageBatch(dis,multicast);
                    for(MessageBatch batch: batches) {
                        if(batch != null)
                            for(Message msg: batch)
                                retval.add(msg);
                    }
                }
                else {
                    Message msg=TP.readMessage(dis);
                    retval.add(msg);
                }
            }
            return retval;
        }
        catch(Throwable t) {
            t.printStackTrace();
            return null;
        }
        finally {
            Util.close(dis);
        }
    }

    private static void print(Message msg, boolean multicast) {
        System.out.println(msg + ", hdrs: " + msg.printHeaders() + ", mcast: " + multicast);
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

        List<Message> msgs=new ParseMessages(file).parse();
        int cnt=1;
        for(Message msg: msgs)
            System.out.println(cnt++ + ": " + msg + ", hdrs: " + msg.printHeaders());
    }

    static private void help() {
        System.out.println("ParseMessages [-file <filename>]");
    }
}
