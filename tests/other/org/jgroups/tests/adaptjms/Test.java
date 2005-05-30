package org.jgroups.tests.adaptjms;

import org.apache.log4j.Logger;

import javax.jms.*;
import javax.naming.InitialContext;
import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.net.InetAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.StringTokenizer;

/**  You start the test by running this class.
 *    It only parses the initial params from the config.txt
 *    file (or any other file you wish to pass as the argument)
 *    and instantiates a new JGroupsTester object.<br>
 * Use parameters -Xbatch -Xconcurrentio (Solaris specific)
 * @author Milcan Prica (prica@deei.units.it)
 * @author Bela Ban (belaban@yahoo.com)

 */
public class Test {
    public static int mcast_port=7777;
    public static String mcast_addr="228.8.8.8";
    public static int grpMembers=4;

    public static void main(String[] args) {
        String config="config.txt";
        BufferedReader fileReader;
        String line;

        boolean sender=false;
        int num_msgs=1000;
        int msg_size=500;

        int num_senders=1;
        long   log_interval=1000;
        ConnectionFactory factory;
        InitialContext ctx;
        TopicConnection conn;
        TopicSession session;
        TopicPublisher pub;
        Topic topic;
        String topic_name="topic/testTopic";

        for(int i=0; i < args.length; i++) {
            if("-sender".equals(args[i])) {
                sender=true;
                continue;
            }
            if("-receiver".equals(args[i])) {
                sender=false;
                continue;
            }
            if("-config".equals(args[i])) {
                config=args[++i];
                continue;
            }
            help();
            return;
        }



        try {
            fileReader=new BufferedReader(new FileReader(config));
            while((line=fileReader.readLine()) != null) {
                if(line.startsWith("#"))
                    continue;
                else if(line.startsWith("NUM_MSGS=")) {
                    num_msgs=Integer.parseInt(line.substring(line.indexOf('=') + 1,
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
                else if(line.startsWith("TOPIC=")) {
                   topic_name=line.substring(line.indexOf('=') + 1, line.indexOf(';'));
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
                    + "Sender:" + sender + "  num_msgs:" + num_msgs
                    + "  Size(bytes):" + msg_size + "  # Mbrs:" + grpMembers
                    + "  Senders: " + num_senders
                    + "\nLog interval: " + log_interval + '\n';

            System.out.println(s);
            Logger.getLogger(Test.class).info("main(): " + s);

            ctx=new InitialContext();
            factory=(ConnectionFactory)ctx.lookup("ConnectionFactory");
            conn=((TopicConnectionFactory)factory).createTopicConnection();
            session=conn.createTopicSession(false, Session.AUTO_ACKNOWLEDGE);
            topic=(Topic)ctx.lookup(topic_name);
            pub=session.createPublisher(topic);
            new JmsTester(conn, session, topic, pub, sender, num_msgs,
                    msg_size, grpMembers, num_senders,
                    log_interval).initialize();
        }
        catch(FileNotFoundException notFound) {
            log.error("File not found.\n" + notFound);
        }
        catch(IOException ioError) {
            log.error(ioError);
        }
        catch(Exception ex) {
            ex.printStackTrace();
        }
    }



    public static List parseCommaDelimitedList(String s) throws Exception {
        List retval=new ArrayList();
        StringTokenizer tok;
        InetAddress host;

        if(s == null) return null;
        tok=new StringTokenizer(s, ",");
        while(tok.hasMoreTokens()) {
            host=InetAddress.getByName(tok.nextToken());
            retval.add(host);
        }
        return retval;
    }

    static void help() {
        System.out.println("Test [-help] ([-sender] | [-receiver]) [-config <config file>]");
    }
}
