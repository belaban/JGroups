
package org.jgroups.auth;

import org.jgroups.*;
import org.jgroups.annotations.Property;
import org.jgroups.util.NameCache;

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


    public void init() throws Exception {
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
            String logical_name=sender != null? NameCache.get(sender) : null;
            if(logical_name != null) {
                Matcher matcher=pattern.matcher(logical_name);
                boolean result=matcher.matches();
                if(log.isTraceEnabled())
                    log.trace("matching logical_name: pattern= " + pattern + ", input= " + logical_name + ", result= " + result);
                return result;
            }
        }
        return false;
    }

    @Override
    public void writeTo(DataOutput out) {
    }

    @Override
    public void readFrom(DataInput in) {
    }

    public int size() {
        return 0;
    }
}
