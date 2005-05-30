package org.jgroups.tests;

import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;

/**
 * Determine the fragmentation size of a system. Can be used offline, then set properties
 * FRAG.frag_size and NAKACK.max_xmit_size
 * @author Bela Ban Dec 11
 * @author 2003
 * @version $Id: DetermineFragSize.java,v 1.3 2005/05/30 14:31:36 belaban Exp $
 */
public class DetermineFragSize {


    static int senseMaxFragSize() {
        int upper=4096;
        int lower=0;
        int highest_failed=-1;
        DatagramSocket sock;
        byte[] buf;
        DatagramPacket packet;
        InetAddress local_addr;
        final int num_iterations=15;

        try {
            sock=new DatagramSocket();
            local_addr=InetAddress.getLocalHost();
        }
        catch(Exception ex) {
            log.error("failed creating DatagramSocket: " + ex);
            return lower;
        }

        for(int i=0; i < num_iterations && lower < upper; i++) { // iterations to approximate frag_size
            try {
                buf=new byte[upper];
                // System.out.println("** upper=" + upper + " (lower=" + lower + ")");
                packet=new DatagramPacket(buf, buf.length, local_addr, 9);
                sock.send(packet);
                lower=Math.max(lower, upper);
                System.out.println("-- trying " + lower + " [OK]");
                upper=upper * 2;
                if(highest_failed > -1)
                    upper=Math.min(highest_failed, upper);
            }
            catch(IOException io_ex) {
                if(highest_failed > -1)
                    highest_failed=Math.min(highest_failed, upper); // never exceed max_upper
                else
                    highest_failed=upper;
                upper=(upper + lower) / 2;
            }
            catch(Throwable ex) {
                ex.printStackTrace();
                break;
            }
        }
        return lower;
    }


    public static void main(String[] args) {
        DatagramSocket sock;
        DatagramPacket packet;
        int size=0, frag_size=0;
        byte[] buf;

        try {
            size=senseMaxFragSize();
            System.out.println("-- fine tuning (starting at " + size + "):");
            sock=new DatagramSocket();
            for(; ;) {
                buf=new byte[size];
                packet=new DatagramPacket(buf, buf.length, InetAddress.getLocalHost(), 9);
                sock.send(packet);
                // System.out.print(size + " ");
                // System.out.println(size + " [OK]");
                frag_size=size;
                size++;
            }
        }
        catch(Throwable t) {
            // System.out.println(size + " [FAIL]");
            // t.printStackTrace();
        }
        System.out.println("\n***** fragmentation size on your system is " + frag_size + " bytes *******\n");
    }
}
