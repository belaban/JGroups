package org.jgroups.tests;

import java.io.*;

/**
 * Checks whether numbers in a stream are monotonically increasing, e.g.
 * <pre>
 * 1
 * 2
 * 3
 * 4
 * </pre>
 * @author Bela Ban
 */
public class CheckMonotonicallyIncreasingNumbers {

    static int check(InputStream in) throws IOException {
        Reader r = new BufferedReader(new InputStreamReader(in));
        StreamTokenizer st = new StreamTokenizer(r);
        int i, cnt=0, num=0, tmp, incorrect=0;
        boolean first_read=false;

        while(true) {
            i=st.nextToken();
            if(i == StreamTokenizer.TT_EOF)
                break;
            tmp=(int)st.nval;
            if(!first_read) {
                first_read=true;
            }
            else {
                if(tmp != num +1) {
                    System.err.println("Number read: " + tmp + ", previous number: " + num +
                            " (lineno: " + st.lineno() + ")");
                    incorrect++;
                }
            }
            num=tmp;
            cnt++;
            if(cnt > 0 && cnt % 1000 == 0)
                System.out.println("read " + cnt + " numbers");
        }
        return incorrect;
    }


    public static void main(String[] args) throws IOException {
        String file=null;
        for(int i=0; i < args.length; i++) {
            if(args[i].equals("-file")) {
                file=args[++i];
                continue;
            }
            help();
            return;
        }
        FileInputStream fis=new FileInputStream(file);
        int incorrect=CheckMonotonicallyIncreasingNumbers.check(fis);
        if(incorrect == 0) {
            System.out.println("OK, all numbers are monotonically increasing");
        }
        else {
            System.err.println("Failure: there are " + incorrect + " incorrect numbers");
        }
        fis.close();
    }

    private static void help() {
        System.out.println("CheckMonotonicallyIncreasingNumbers [-help] [-file <filename>]");
    }
}
