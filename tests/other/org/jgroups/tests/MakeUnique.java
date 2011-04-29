package org.jgroups.tests;

import org.jgroups.util.Util;

import java.io.*;
import java.util.*;

/**
 * Replaces words that start with args passed to thei program with A, B, C and so on. Useful to convert a view of
 * long names into shorter ones, e.g. for parsing logs
 * @author Bela Ban
 * @since 3.0
 */
public class MakeUnique {
    
    static void start(String inputfile, String outputfile, String delimiters, Collection<String> keywords) throws IOException {
        String input=inputfile != null? Util.readFile(inputfile) : Util.readContents(System.in);
        String delims=",\n\r \t[]";
        if(delimiters != null)
            delims=delims + delimiters;
        
        StringTokenizer tok=new StringTokenizer(input, delims, true);
        FileOutputStream output=new FileOutputStream(outputfile);

        Map<String,Integer> map=new HashMap<String,Integer>();
        int current_char='A';

        while(tok.hasMoreTokens()) {
            String token=tok.nextToken();
            if(token == null)
                continue;

            // check if token is already in the map
            if(map.containsKey(token)) {
                Integer val=map.get(token);
                output.write((char)val.intValue());
                System.out.print((char)val.intValue());
                continue;
            }

            if(keywords != null && isKeyword(keywords, token)) {
                map.put(token, current_char++);
                Integer val=map.get(token);
                output.write((char)val.intValue());
                System.out.print((char)val.intValue());
            }
            else {
                output.write(token.getBytes());
                System.out.print(new String(token.getBytes()));
            }
        }
        output.close();
        System.out.println("\noutput written to " + outputfile);
    }

    static boolean isKeyword(Collection<String> keywords, String token) {
        // full match
        for(String keyword: keywords) {
            if(token.equals(keyword))
                return true;
        }

        // partial
        for(String keyword: keywords) {
            if(token.startsWith(keyword))
                return true;
        }
        return false;
    }

    public static void main(String[] args) throws IOException {
        String input=null;
        String output="output.txt";
        String delims=null;
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
            if(args[i].equals("-delims")) {
                delims=args[++i];
                continue;
            }
            if(args[i].equals("-h")) {
                System.out.println("MakeUnique -in inputfile [-out outputfile] [-delims delimiters] [keyword]*");
                return;
            }
            keywords.add(args[i]);
        }
        start(input, output, delims, keywords);
    }
}
