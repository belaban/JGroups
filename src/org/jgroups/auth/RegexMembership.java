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

import org.jgroups.Address;
import org.jgroups.Event;
import org.jgroups.Message;
import org.jgroups.PhysicalAddress;
import org.jgroups.annotations.Property;
import org.jgroups.util.UUID;

import java.io.DataInput;
import java.io.DataOutput;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Matches the IP address or logical name of a joiner against a regular expression and accepts or rejects based on
 * pattern matching
 * @author Bela Ban
 */
public class RegexMembership extends AuthToken {

    @Property(description="The regular expression against which the IP address or logical host of a joiner will be matched")
    protected String  match_string=null;

    @Property(description="Matches the IP address of the joiner against the match string")
    protected boolean match_ip_address=true;

    @Property(description="Matches the logical name of the joiner against the match string")
    protected boolean match_logical_name=false;


    // ------------------------------------------- Fields ------------------------------------------------------ //

    protected Pattern pattern;


    public RegexMembership() {
    }


    public String getName() {
        return "org.jgroups.auth.RegexMembership";
    }


    public void init() {
        super.init();
        if(!match_ip_address && !match_logical_name)
            throw new IllegalArgumentException("either match_ip_address or match_logical_address has to be true");
        if(match_string == null)
            throw new IllegalArgumentException("match_string cannot be null");
        pattern=Pattern.compile(match_string);
    }


    public boolean authenticate(AuthToken token, Message msg) {
        Address sender=msg.getSrc();


        if(match_ip_address) {
            PhysicalAddress src=sender != null? (PhysicalAddress)auth.down(new Event(Event.GET_PHYSICAL_ADDRESS, sender)) : null;
            String ip_addr=src != null? src.toString() : null;
            if(ip_addr != null) {
                Matcher matcher=pattern.matcher(ip_addr);
                boolean result=matcher.matches();
                if(log.isTraceEnabled())
                    log.trace("matching ip_address: pattern= " + pattern + ", input= " + ip_addr + ", result= " + result);
                if(result)
                    return true;
            }
        }
        if(match_logical_name) {
            String logical_name=sender != null? UUID.get(sender) : null;
            if(logical_name != null) {
                Matcher matcher=pattern.matcher(logical_name);
                boolean result=matcher.matches();
                if(log.isTraceEnabled())
                    log.trace("matching logical_name: pattern= " + pattern + ", input= " + logical_name + ", result= " + result);
                if(result)
                    return true;
            }
        }
        return false;
    }

   

    public void writeTo(DataOutput out) throws Exception {
    }

    /**
     * Required to deserialize the object when read in from the wire
     *
     *
     * @param in
     * @throws Exception
     */
    public void readFrom(DataInput in) throws Exception {
    }
}
