package org.jgroups.tests;

import java.io.*;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

/**
 * Replaces words that start with args passed to thei program with A, B, C and so on. Useful to convert a view of
 * long names into shorter ones, e.g. for parsing logs
 * @author Bela Ban
 * @since 3.0
 */
public class MakeUnique {
    
    static void start(String inputfile, String outputfile, Collection<String> keywords) throws IOException {
        InputStream in=inputfile == null? System.in : new FileInputStream(inputfile);

        BufferedReader reader=new BufferedReader(new InputStreamReader(in));
        StreamTokenizer tok=new StreamTokenizer(reader);
        tok.slashSlashComments(false);
        tok.slashStarComments(false);
        // tok.parseNumbers();
        tok.wordChars((int)'0', (int)'9');
        tok.wordChars('-', '.');

        while(true) {
            int type=tok.nextToken();
            if(type == StreamTokenizer.TT_EOF)
                break;
            System.out.println(tok.sval);
        }

    }

    public static void main(String[] args) throws IOException {
        String input=null;
        String output="output.txt";
        Set<String> keywords=new HashSet<String>();
        for(int i=0; i < args.length; i++) {
            if(args[i].equals("-in")) {
                input=args[++i];
                continue;
            }
            if(args[i].equals("-out")) {
                output=args[++i];
                continue;
            }
            if(args[i].equals("-h")) {
                System.out.println("MakeUnique -in inputfile [-out outputfile] [keyword]*");
                return;
            }
            keywords.add(args[i]);
        }
        start(input, output, keywords);
    }
}
