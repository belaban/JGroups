package org.jgroups.tests;

import org.jgroups.util.Util;

import java.net.InetAddress;
import java.net.SocketException;

/**
 * Returns a global IP address, if not found a site-local, if not found a non loopback, if not found the loopback addr
 * @author Bela Ban
 * @version $Id: GetGlobalAddress.java,v 1.1 2010/05/28 08:53:10 belaban Exp $
 */
public class GetGlobalAddress {
    public static void main(String[] args) {
        try {
            InetAddress addr=Util.getAddress(Util.AddressScope.GLOBAL);
            if(addr != null) {
                System.out.println(addr.getHostAddress());
                return;
            }
            addr=Util.getAddress(Util.AddressScope.SITE_LOCAL);
            if(addr != null) {
                System.out.println(addr.getHostAddress());
                return;
            }
            addr=Util.getAddress(Util.AddressScope.NON_LOOPBACK);
            if(addr != null) {
                System.out.println(addr.getHostAddress());
                return;
            }
            addr=Util.getAddress(Util.AddressScope.LOOPBACK);
            if(addr != null) {
                System.out.println(addr.getHostAddress());
            }
        }
        catch(SocketException e) {
        }
    }
}
