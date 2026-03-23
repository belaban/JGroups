package org.jgroups.util;

import java.net.InetAddress;
import java.net.NetworkInterface;
import java.net.SocketException;
import java.util.Enumeration;

/**
 * Lists all network interfaces on a system
 * @author Bela Ban Dec 18
 * @author 2003
 */
public class GetNetworkInterfaces {

    public static void main(String[] args) throws SocketException {
        Enumeration<NetworkInterface> en=NetworkInterface.getNetworkInterfaces();
        while(en.hasMoreElements()) {
            NetworkInterface i=en.nextElement();
            boolean up=Util.isUp(i);
            if(!up)
                continue;
            System.out.printf("%s (%s)\n", i.getName(), up? "up" : "down");
            for(Enumeration<InetAddress> en2=i.getInetAddresses(); en2.hasMoreElements();) {
                InetAddress addr=en2.nextElement();
                System.out.println("  \t" + addr);
            }
            System.out.println("---------------------");
        }
    }

}
