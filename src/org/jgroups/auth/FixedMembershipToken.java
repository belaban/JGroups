/*
* JBoss, Home of Professional Open Source
* Copyright 2005, JBoss Inc., and individual contributors as indicated
* by the @authors tag. See the copyright.txt in the distribution for a
* full listing of individual contributors.
*
* This is free software; you can redistribute it and/or modify it
* under the terms of the GNU Lesser General Public License as
* published by the Free Software Foundation; either version 2.1 of
* the License, or (at your option) any later version.
*
* This software is distributed in the hope that it will be useful,
* but WITHOUT ANY WARRANTY; without even the implied warranty of
* MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
* Lesser General Public License for more details.
*
* You should have received a copy of the GNU Lesser General Public
* License along with this software; if not, write to the Free
* Software Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA
* 02110-1301 USA, or see the FSF site: http://www.fsf.org.
*/
package org.jgroups.auth;

import org.jgroups.Message;
import org.jgroups.Event;
import org.jgroups.Address;
import org.jgroups.util.Util;

import java.util.Properties;
import java.util.ArrayList;
import java.util.List;
import java.util.StringTokenizer;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.DataInputStream;

/**
 * <p>
 * The FixedMemberShipToken object predefines a list of IP addresses and Ports that can join the group.
 *</p>
 * <p>
 * Configuration parameters for this example are shown below:
 * </p>
 * <ul>
 *  <li>fixed_members_value (required) = List of IP addresses & ports (optionally) - ports must be seperated by a '/' e.g. 127.0.0.1/1010*127.0.0.1/4567</li>
 *  <li>fixed_members_seperator (required) = The seperator used between IP addresses - e.g. *</li>
 * </ul>
 * @author Chris Mills (millsy@jboss.com)
 */
public class FixedMembershipToken extends AuthToken {
    private static final String FIXED_MEMBERS_ATTR = "fixed_members_value";
    private static final String FIXED_MEMBERS_SEPERATOR_ATTR = "fixed_members_seperator";

    private List<String> memberList = null;
    private String token = "emptyToken";
    private static final long serialVersionUID=-8222380233642107792L;

    public FixedMembershipToken(){
    }

    public String getName(){
        return "org.jgroups.auth.FixedMembershipToken";
    }



    public boolean authenticate(AuthToken token, Message msg) {
        if((token != null) && (token instanceof FixedMembershipToken) && (this.memberList != null)) {
            Address src=msg.getSrc();
            String sourceAddressWithPort=src.toString();
            String sourceAddressWithoutPort=sourceAddressWithPort.substring(0, sourceAddressWithPort.indexOf(":"));

            if(log.isDebugEnabled()) {
                log.debug("AUTHToken received from " + sourceAddressWithPort);
            }

            for(String member: memberList) {
                if(hasPort(member)) {
                    if(member.equals(sourceAddressWithPort))
                        return true;
                }
                else {
                    if(member.equals(sourceAddressWithoutPort))
                        return true;
                }
            }
            return false;
        }

        if(log.isWarnEnabled()) {
            log.warn("Invalid AuthToken instance - wrong type or null");
        }
        return false;
    }

    private static boolean hasPort(String member) {
        return member.contains(":");
    }

    public void setValue(Properties properties){
        memberList = new ArrayList<String>();
        StringTokenizer memberListTokenizer = new StringTokenizer((String)properties.get(FixedMembershipToken.FIXED_MEMBERS_ATTR),
                (String)properties.get(FixedMembershipToken.FIXED_MEMBERS_SEPERATOR_ATTR));
        while(memberListTokenizer.hasMoreTokens()){
            memberList.add(memberListTokenizer.nextToken().replace('/', ':'));
        }
        properties.remove(FixedMembershipToken.FIXED_MEMBERS_ATTR);
        properties.remove(FixedMembershipToken.FIXED_MEMBERS_SEPERATOR_ATTR);
    }
    /**
     * Required to serialize the object to pass across the wire
     * @param out
     * @throws java.io.IOException
     */
    public void writeTo(DataOutputStream out) throws IOException {
        if(log.isDebugEnabled()){
            log.debug("SimpleToken writeTo()");
        }
        Util.writeString(this.token, out);
    }
    /**
     * Required to deserialize the object when read in from the wire
     * @param in
     * @throws IOException
     * @throws IllegalAccessException
     * @throws InstantiationException
     */
    public void readFrom(DataInputStream in) throws IOException, IllegalAccessException, InstantiationException {
        if(log.isDebugEnabled()){
            log.debug("SimpleToken readFrom()");
        }
        this.token = Util.readString(in);
    }
}
