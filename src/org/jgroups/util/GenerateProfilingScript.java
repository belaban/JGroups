package org.jgroups.util;

import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.stream.Stream;

/**
 * Takes a list of methods to profile and generates the Byteman script from them. See ./conf/scripts/profiled-methods.txt
 * as an example
 * @author Bela Ban
 * @since  5.2.13
 */
public class GenerateProfilingScript {
    protected int num_rules_generated;

    protected void generate(String input_file, String output_file) throws IOException {
        try(OutputStream out=new FileOutputStream(output_file); Stream<String> lines=Files.lines(Path.of(input_file))) {
            out.write(PREFIX.getBytes());
            lines.filter(l -> !l.trim().isEmpty())
              .forEach(l -> generateRule(l, out));
        }
    }

    protected void generateRule(String method, OutputStream out) {
        int index=method.lastIndexOf('.');
        if(index < 0)
            throw new IllegalArgumentException(String.format("method name not found in %s", method));
        String classname=method.substring(0, index).trim(), method_name=method.substring(index+1).trim();
        try {
            Class<?> clazz=Util.loadClass(classname, (Class<?>)null);
            boolean is_interface=clazz.isInterface();
            String start_rule=String.format(START_RULE, clazz.getSimpleName() + "." + method_name,
                                            is_interface? "INTERFACE" : "CLASS",
                                            classname, method_name, clazz.getSimpleName() + "." + method_name);
            String stop_rule=String.format(STOP_RULE, clazz.getSimpleName() + "." + method_name,
                                           is_interface? "INTERFACE" : "CLASS",
                                           classname, method_name, clazz.getSimpleName() + "." + method_name);

            out.write(start_rule.getBytes());
            out.write(stop_rule.getBytes());
        }
        catch(Exception e) {
            throw new RuntimeException(e);
        }
    }

    public static void main(String[] args) throws IOException {
        if(args.length != 2) {
            System.err.printf("%s <input file> <output file>\n", GenerateProfilingScript.class.getSimpleName());
            return;
        }
        String input=args[0], output=args[1];
        GenerateProfilingScript tmp=new GenerateProfilingScript();
        tmp.generate(input, output);
    }



    protected static final String PREFIX="""
      # Byteman script to profile individual methods
      
      RULE DiagnosticHandler creation
      CLASS ^TP
      HELPER org.jgroups.util.ProfilingHelper
      METHOD handleConnect()
      AT ENTRY
      BIND tp=$this, diag=tp.getDiagnosticsHandler();
      IF TRUE
         DO diagCreated(diag);
      ENDRULE
      
      """;


    protected static final String START_RULE="""
      RULE %s start
      %s %s
      HELPER org.jgroups.util.ProfilingHelper
      METHOD %s
      COMPILE
      AT ENTRY
      IF TRUE
         DO start("%s");
      ENDRULE
      
      """;

    protected static final String STOP_RULE="""
      RULE %s stop
      %s %s
      HELPER org.jgroups.util.ProfilingHelper
      METHOD %s
      COMPILE
      AT EXIT
      IF TRUE
         DO stop("%s");
      ENDRULE
      """;
}
