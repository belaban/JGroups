package org.jgroups.tests;

import org.jgroups.util.Util;

import java.io.*;
import java.util.stream.Stream;

/**
 * @author Bela Ban
 * @since  4.0.16
 */
public class HexToBinary {


    public static void main(String[] args) throws Exception {
        if(args.length != 2) {
            System.err.printf("%s <input file> <output file>\n", HexToBinary.class.getSimpleName());
            return;
        }

        File input_file=new File(args[0]);
        File output_file=new File(args[1]);

        if(!input_file.exists())
            throw new FileNotFoundException(input_file.toString());
        if (!output_file.exists())
            output_file.createNewFile();
        FileWriter fw = new FileWriter(output_file.getAbsoluteFile());
        BufferedWriter bw = new BufferedWriter(fw);

        BufferedReader br=null;
        try {
            br = new BufferedReader(new FileReader(input_file));

            Stream<String> lines=br.lines();
            lines.forEach(sCurrentLine -> {
                if(sCurrentLine != null && !sCurrentLine.isEmpty()) {
                    String bits=Util.hexToBin(sCurrentLine);
                    try {
                        bw.write(bits);
                    }
                    catch(IOException e) {
                        e.printStackTrace();
                    }
                }
            });

           // while ((sCurrentLine = br.readLine(true)) != null) {
             //   bits = Util.hexToBin(sCurrentLine);
               // bw.write(bits);
            //}


            bw.close();
            System.out.println("done....");

        }
        catch (Throwable e) {
            e.printStackTrace();
        }
        finally {
            try {
                if (br != null)br.close();
            } catch (IOException ex) {
                ex.printStackTrace();
            }
        }
    }


}
