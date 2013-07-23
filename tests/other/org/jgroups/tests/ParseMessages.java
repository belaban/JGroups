package org.jgroups.tests;

import org.jgroups.Message;
import org.jgroups.Version;
import org.jgroups.protocols.TP;
import org.jgroups.util.MessageBatch;
import org.jgroups.util.Util;

import java.io.*;
import java.util.Arrays;

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
                try {
                    version=dis.readShort();
                }
                catch(IOException io_ex) {
                    break;
                }

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
                    final MessageBatch[] batches=TP.readMessageBatch(dis,multicast);
                    final MessageBatch batch=batches[0], oob_batch=batches[1],
                      internal_batch_oob=batches[2], internal_batch=batches[3];
                    int size=batch != null? batch.size() : 0;
                    if(oob_batch != null)
                        size+=oob_batch.size();
                    if(internal_batch_oob != null)
                        size+=internal_batch_oob.size();
                    if(internal_batch != null)
                        size+=internal_batch.size();

                    System.out.println(size + " msgs: ");

                    int cnt=1;

                    for(MessageBatch tmp: Arrays.asList(batch, oob_batch, internal_batch_oob, internal_batch)) {
                        if(tmp != null) {
                            for(Message msg: tmp) {
                                System.out.print("#" + cnt++ + ": ");
                                print(msg, multicast);
                            }
                        }
                    }
                }
                else {
                    Message msg=TP.readMessage(dis);
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
