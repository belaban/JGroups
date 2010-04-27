package org.jgroups.util;

import java.net.*;
import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Default implementation, ignores service names
 * @author Bela Ban
 * @version $Id: DefaultSocketFactory.java,v 1.1 2010/04/27 09:27:32 belaban Exp $
 */
public class DefaultSocketFactory implements SocketFactory {
    // Maintains information about open sockets
    protected final Map<Object,String> sockets=new ConcurrentHashMap<Object,String>();

    public Socket createSocket(String service_name) throws IOException {
        return add(new Socket(), service_name);
    }

    public Socket createSocket(String service_name, String host, int port) throws IOException {
        return add(new Socket(host, port), service_name);
    }

    public Socket createSocket(String service_name, InetAddress address, int port) throws IOException {
        return add(new Socket(address, port), service_name);
    }

    public Socket createSocket(String service_name, String host, int port, InetAddress localAddr, int localPort) throws IOException {
        return add(new Socket(host, port, localAddr, localPort), service_name);
    }

    public Socket createSocket(String service_name, InetAddress address, int port, InetAddress localAddr, int localPort) throws IOException {
        return add(new Socket(address, port, localAddr, localPort), service_name);
    }

    public ServerSocket createServerSocket(String service_name) throws IOException {
        return add(new ServerSocket(), service_name);
    }

    public ServerSocket createServerSocket(String service_name, int port) throws IOException {
        return add(new ServerSocket(port), service_name);
    }

    public ServerSocket createServerSocket(String service_name, int port, int backlog) throws IOException {
        return add(new ServerSocket(port, backlog), service_name);
    }

    public ServerSocket createServerSocket(String service_name, int port, int backlog, InetAddress bindAddr) throws IOException {
        return add(new ServerSocket(port, backlog, bindAddr), service_name);
    }

    public DatagramSocket createDatagramSocket(String service_name) throws SocketException {
        return add(new DatagramSocket(), service_name);
    }

    public DatagramSocket createDatagramSocket(String service_name, SocketAddress bindaddr) throws SocketException {
        return add(new DatagramSocket(bindaddr), service_name);
    }

    public DatagramSocket createDatagramSocket(String service_name, int port) throws SocketException {
        return add(new DatagramSocket(port), service_name);
    }

    public DatagramSocket createDatagramSocket(String service_name, int port, InetAddress laddr) throws SocketException {
        return add(new DatagramSocket(port, laddr), service_name);
    }

    public MulticastSocket createMulticastSocket(String service_name) throws IOException {
        return add(new MulticastSocket(), service_name);
    }

    public MulticastSocket createMulticastSocket(String service_name, int port) throws IOException {
        return add(new MulticastSocket(port), service_name);
    }

    public MulticastSocket createMulticastSocket(String service_name, SocketAddress bindaddr) throws IOException {
        return add(new MulticastSocket(bindaddr), service_name);
    }

    public void close(Socket sock) throws IOException {
        remove(sock);
        Util.close(sock);
    }

    public void close(ServerSocket sock) throws IOException {
        remove(sock);
        Util.close(sock);
    }

    public void close(DatagramSocket sock) {
        remove(sock);
        Util.close(sock);
    }

    public Map<Object, String> getSockets() {
        return sockets;
    }



    protected <T> T add(T sock, String service_name) {
        if(sock != null) {
            String tmp=service_name == null? "n/a" : service_name;
            sockets.put(sock, tmp);
        }
        return sock;
    }

    protected <T> void remove(T sock) {
        if(sock != null)
            sockets.remove(sock);
    }
}
