package org.jgroups.tests.adapt;

import org.apache.log4j.Logger;
import org.jgroups.log.Trace;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;

/**  You start the test by running this class.
 *    It only parses the initial params from the config.txt
 *    file (or any other file you wish to pass as the argument)
 *    and instantiates a new JGroupsTester object.<br>
 * Use parameters -Xbatch -Xconcurrentio (Solaris specific)
 * @author Milcan Prica (prica@deei.units.it)
 * @author Bela Ban (belaban@yahoo.com)

 */
public class Test {

    public static void main(String[] args) {

        String config;
        BufferedReader fileReader;
        String line;

        boolean sender=false;
        long msgs_burst=50;
        long sleep_msec=50;
        int num_bursts=50;
        int msg_size=500;
        int grpMembers=4;
        int num_senders=1;
        String props=null;
        long   log_interval=1000;

        try {
            config=args[0];
        }
        catch(Exception e) {
            config="config.txt";
        }

        Trace.init();

        try {
            fileReader=new BufferedReader(new FileReader(config));
            while((line=fileReader.readLine()) != null) {
                if(line.startsWith("#"))
                    continue;
                else if(line.startsWith("SENDER=")) {
                    sender=new Boolean(line.substring(line.indexOf('=') + 1,
                            line.indexOf(';'))).booleanValue();
                }
                else if(line.startsWith("MSGS_BURST=")) {
                    msgs_burst=Long.parseLong(line.substring(line.indexOf('=') + 1,
                            line.indexOf(';')));
                }
                else if(line.startsWith("SLEEP_MSEC=")) {
                    sleep_msec=Long.parseLong(line.substring(line.indexOf('=') + 1,
                            line.indexOf(';')));
                }
                else if(line.startsWith("NUM_BURSTS=")) {
                    num_bursts=Integer.parseInt(line.substring(line.indexOf('=') + 1,
                            line.indexOf(';')));
                }
                else if(line.startsWith("MSG_SIZE=")) {
                    msg_size=Integer.parseInt(line.substring(line.indexOf('=') + 1,
                            line.indexOf(';')));
                }
                else if(line.startsWith("GRPMEMBERS=")) {
                    grpMembers=Integer.parseInt(line.substring(line.indexOf('=') + 1,
                            line.indexOf(';')));
                }
                else if(line.startsWith("NUM_SENDERS=")) {
                    num_senders=Integer.parseInt(line.substring(line.indexOf('=') + 1,
                            line.indexOf(';')));
                }
                else if(line.startsWith("LOG_INTERVAL=")) {
                    log_interval=Long.parseLong(line.substring(line.indexOf('=') + 1,
                            line.indexOf(';')));
                }
                else if(line.startsWith("PROPS=")) {
                    props=line.substring(line.indexOf('=') + 1,
                            line.indexOf(';')).trim();
                }
                else if(line.startsWith("GNUPLOT_OUTPUT=")) {
                    // only parse if not yet set by -Dgnuplot_output=true option (overrides file)
                    if(System.getProperty("gnuplot_output") == null) {
                        String gnuplot_output=line.substring(line.indexOf('=') + 1,
                                line.indexOf(';')).trim();
                        if(gnuplot_output != null)
                            System.setProperty("gnuplot_output", gnuplot_output);
                    }
                }
            }
            fileReader.close();

            System.out.println("Javagroups test:");
            String s="Initial params parsing completed. Starting test"
                    + " with these values:\n"
                    + "Sender:" + sender + "  Msgs/Burst:" + msgs_burst
                    + "  Sleep(ms):" + sleep_msec + "  # Bursts:" + num_bursts
                    + "  Size(bytes):" + msg_size + "  # Mbrs:" + grpMembers
                    + "  Senders: " + num_senders
                    + "\nLog interval: " + log_interval + "\n";

            System.out.println(s);
            Logger.getLogger(Test.class).info("main(): " + s);

            new JGroupsTester(sender, msgs_burst, sleep_msec,
                    num_bursts, msg_size, grpMembers, num_senders, props, log_interval).initialize();
        }
        catch(FileNotFoundException notFound) {
            System.err.println("File not found.\n" + notFound);
        }
        catch(IOException ioError) {
            System.err.println(ioError);
        }
    }
}
