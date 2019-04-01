package org.jgroups.util;

import java.io.IOException;
import java.net.*;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.Map;

/**
 * Factory to create various types of sockets. For socket creation, a <em>service name</em> can be passed as argument:
 * an implementation could look up a service description (e.g. port) and create the socket, ignoring the passed port and
 * possibly also the bind address.<p/>
 * Ephemeral ports can be created by passing 0 as port, or (if the port is ignored), an implementation could pass in
 * a special service name (e.g. "EPHEMERAL"), this is implementation dependent.<p/>
 * The socket creation methods have the same parameter lists as the socket constructors, e.g.
 * {@link #createServerSocket(String, int, int)} is the same as {@link java.net.ServerSocket#ServerSocket(int, int)}.
 * @author Bela Ban
 */
public interface SocketFactory {
    Socket createSocket(String service_name) throws IOException;
    Socket createSocket(String service_name, String host, int port) throws IOException;
    Socket createSocket(String service_name, InetAddress address, int port) throws IOException;
    Socket createSocket(String service_name, String host, int port, InetAddress localAddr, int localPort) throws IOException;
    Socket createSocket(String service_name, InetAddress address, int port, InetAddress localAddr, int localPort) throws IOException;

    ServerSocket createServerSocket(String service_name) throws IOException;
    ServerSocket createServerSocket(String service_name, int port) throws IOException;
    ServerSocket createServerSocket(String service_name, int port, int backlog) throws IOException;
    ServerSocket createServerSocket(String service_name, int port, int backlog, InetAddress bindAddr) throws IOException;

    default SocketChannel createSocketChannel(String service_name) throws IOException {
        return SocketChannel.open();
    }

    default SocketChannel createSocketChannel(String service_name, SocketAddress bindAddr) throws IOException {
        return this.createSocketChannel(service_name).bind(bindAddr);
    }

    default ServerSocketChannel createServerSocketChannel(String service_name) throws IOException {
        return ServerSocketChannel.open();
    }

    default ServerSocketChannel createServerSocketChannel(String service_name, int port) throws IOException {
        return createServerSocketChannel(service_name).bind(new InetSocketAddress(port));
    }

    default ServerSocketChannel createServerSocketChannel(String service_name, int port, int backlog) throws IOException {
        return createServerSocketChannel(service_name).bind(new InetSocketAddress(port), backlog);
    }

    default ServerSocketChannel createServerSocketChannel(String service_name, int port, int backlog, InetAddress bindAddr) throws IOException {
        return createServerSocketChannel(service_name).bind(new InetSocketAddress(bindAddr, port), backlog);
    }

    DatagramSocket createDatagramSocket(String service_name) throws SocketException;
    DatagramSocket createDatagramSocket(String service_name, SocketAddress bindaddr) throws SocketException;
    DatagramSocket createDatagramSocket(String service_name, int port) throws SocketException;
    DatagramSocket createDatagramSocket(String service_name, int port, InetAddress laddr) throws SocketException;

    MulticastSocket createMulticastSocket(String service_name) throws IOException;
    MulticastSocket createMulticastSocket(String service_name, int port) throws IOException;
    MulticastSocket createMulticastSocket(String service_name, SocketAddress bindaddr) throws IOException;

    void close(Socket sock) throws IOException;
    void close(ServerSocket sock) throws IOException;
    void close(DatagramSocket sock);
    default void close(SocketChannel channel) {
        Util.close(channel);
    }
    default void close(ServerSocketChannel channel) {
        Util.close(channel);
    }

    /**
     * Returns all open sockets. This method can be used to list or close all open sockets.
     * @return A map of open sockets; keys are Sockets, ServerSockets, DatagramSockets or MulticastSockets, values are
     * the service names.
     */
    Map<Object,String> getSockets();
}
