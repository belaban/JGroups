package org.jgroups.util;

import java.io.IOException;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.net.MulticastSocket;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketAddress;
import java.net.SocketException;
import java.util.Map;

/**
 * Default implementation, ignores service names
 * @author Bela Ban
 */
public class DefaultSocketFactory implements SocketFactory {

    public Socket createSocket(String service_name) throws IOException {
        return new Socket();
    }

    public Socket createSocket(String service_name, String host, int port) throws IOException {
        return new Socket(host, port);
    }

    public Socket createSocket(String service_name, InetAddress address, int port) throws IOException {
        return new Socket(address, port);
    }

    public Socket createSocket(String service_name, String host, int port, InetAddress localAddr, int localPort) throws IOException {
        return new Socket(host, port, localAddr, localPort);
    }

    public Socket createSocket(String service_name, InetAddress address, int port, InetAddress localAddr, int localPort) throws IOException {
        return new Socket(address, port, localAddr, localPort);
    }

    public ServerSocket createServerSocket(String service_name) throws IOException {
        return new ServerSocket();
    }

    public ServerSocket createServerSocket(String service_name, int port) throws IOException {
        return new ServerSocket(port);
    }

    public ServerSocket createServerSocket(String service_name, int port, int backlog) throws IOException {
        return new ServerSocket(port, backlog);
    }

    public ServerSocket createServerSocket(String service_name, int port, int backlog, InetAddress bindAddr) throws IOException {
        return new ServerSocket(port, backlog, bindAddr);
    }

    public DatagramSocket createDatagramSocket(String service_name) throws SocketException {
        return new DatagramSocket();
    }

    public DatagramSocket createDatagramSocket(String service_name, SocketAddress bindaddr) throws SocketException {
        return new DatagramSocket(bindaddr);
    }

    public DatagramSocket createDatagramSocket(String service_name, int port) throws SocketException {
        return new DatagramSocket(port);
    }

    public DatagramSocket createDatagramSocket(String service_name, int port, InetAddress laddr) throws SocketException {
        return new DatagramSocket(port, laddr);
    }

    public MulticastSocket createMulticastSocket(String service_name) throws IOException {
        return new MulticastSocket();
    }

    public MulticastSocket createMulticastSocket(String service_name, int port) throws IOException {
        return new MulticastSocket(port);
    }

    public MulticastSocket createMulticastSocket(String service_name, SocketAddress bindaddr) throws IOException {
        return new MulticastSocket(bindaddr);
    }

    public void close(Socket sock) throws IOException {
        Util.close(sock);
    }

    public void close(ServerSocket sock) throws IOException {
        Util.close(sock);
    }

    public void close(DatagramSocket sock) {
        Util.close(sock);
    }

    public Map<Object,String> getSockets() {
        return null;
    }


}
