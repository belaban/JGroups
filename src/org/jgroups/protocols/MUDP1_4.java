package org.jgroups.protocols;

import org.jgroups.stack.Protocol;

/**
 * Multicast transport. Similar to UDP, but binds to multiple (or all) interfaces for sending and receiving
 * multicast and unicast traffic.<br/>
 * The list of interfaces can be set via a property (comma-delimited list of IP addresses or "all" for all
 * interfaces). Note that this class only works under JDK 1.4 and higher.<br/>
 * For each of the interfaces listed we create a Listener, which listens on the group multicast address and creates
 * a unicast datagram socket. The address of this member is determined at startup time, and is the host name plus
 * a timestamp (LogicalAddress). It does not change during the lifetime of the process. The LogicalAddress contains
 * a list of all unicast socket addresses to which we can send back unicast messages. When we send a message, the
 * Listener adds the sender's return address. When we receive a message, we add that address to our routing cache, which
 * contains logical addresses and physical addresses. When we need to send a unicast address, we first check whether
 * the logical address has a physical address associated with it in the cache. If so, we send a message to that address.
 * If not, we send the unicast message to <em>all</em> physical addresses contained in the LogicalAddress.<br/>
 * MUDP1_4 guarantees that - in scenarios with multiple subnets and multi-homed machines - members do see each other.
 * There is some overhead in multicasting the same message on multiple interfaces, and potentially sending a unicast
 * on multiple interfaces as well, but the advantage is that we don't need stuff like bind_addr any longer. Plus,
 * the unicast routing caches should ensure that unicasts are only sent via 1 interface in almost all cases.
 * @author Bela Ban Oct 2003
 * @version $Id: MUDP1_4.java,v 1.1 2003/10/20 02:43:56 belaban Exp $
 */
public class MUDP1_4 extends Protocol {

    final String name="MUDP1_4";

    public String getName() {
        return name;
    }
}
