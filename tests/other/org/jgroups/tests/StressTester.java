package org.jgroups.tests;

import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.URL;

/**
 * Test driver for testing JGroups-based HttpSession replication.
 *
 * @author Brian Stansberry
 * @version $Id: StressTester.java,v 1.4.6.1 2008/01/22 10:01:29 belaban Exp $
 */
public class StressTester implements Runnable
{
   private URL url;
   private String name;

   StressTester(URL url, String name)
   {
      this.url = url;
      this.name = name;
   }

   public void run()
   {
      System.out.println(name + " starting");

      String cookie = null;
      int count = 0;
      while (true)
      {
         count++;

         try
         {
            HttpURLConnection conn = (HttpURLConnection) url.openConnection();
            if (cookie != null)
               conn.addRequestProperty("Cookie", cookie);
            conn.connect();

            int rc = conn.getResponseCode();

            if (cookie == null)
            {
               String header = conn.getHeaderField("Set-Cookie");
               if (header != null)
               {
                  int semi = header.indexOf(';');
                  if (semi > 0)
                  {
                     cookie = header.substring(0, semi);
                  }
                  else
                  {
                     cookie = header;
                  }
               }
            }
            InputStream is = conn.getInputStream();
             while (is.read() != -1) {
                 ;
             }
             is.close();

            if (rc != 200 || count % 1000 == 0)
            {
               StringBuilder sb = new StringBuilder(name);
               sb.append('-');
               sb.append(count);
               sb.append('-');
               sb.append(rc);
               sb.append('-');
               sb.append(cookie);
               System.out.println(sb.toString());
            }
         }
         catch (Exception e)
         {
            System.out.println(e.getMessage());
         }

      }
   }

   /**
    * Args are:
    *
    * 0) # of client threads to create
    * 1) hostname:port of node 0; e.g 192.168.1.164:8080
    * 2) hostname:port of node 1
    * x) and so on for as many servers as there are
    *
    * @param args
    */
   public static void main(String[] args)
   {
      try
      {
         int threadCount = Integer.parseInt(args[0]);

         int serverCount = args.length - 1;

         URL[] urls = new URL[serverCount];
         for (int i = 1; i < args.length; i++)
         {
            urls[i -1] = new URL("http://" + args[i] + "/jbento-httpsession/SetListOfString16");
         }


         for (int i = 0; i < threadCount; i++)
         {
            StressTester tester = new StressTester(urls[i % serverCount], "Thread"+i);
            Thread t = new Thread(tester);
            t.start();
         }
      }
      catch (Exception e)
      {
         e.printStackTrace();
      }
   }

}
