package org.jgroups.tests;

import org.jgroups.Message;
import org.jgroups.util.Util;

import java.io.ByteArrayInputStream;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.util.List;

/**
 * Parses messages out of a captured file and writes them to stdout
 * @author Bela Ban
 */
public class ParseMessages {

    public static List<Message> parse(byte[] buf, int offset, int length) {
        return Util.parse(new ByteArrayInputStream(buf, offset, length));
    }

    public static List<Message> parse(String filename) throws FileNotFoundException {
        return Util.parse(new FileInputStream(filename));
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

        List<Message> msgs=parse(file);
        int cnt=1;
        for(Message msg: msgs)
            System.out.println(cnt++ + ": " + msg + ", hdrs: " + msg.printHeaders());
    }

    static private void help() {
        System.out.println("ParseMessages [-file <filename>]");
    }
}
