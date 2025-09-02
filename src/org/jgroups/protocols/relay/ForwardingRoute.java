package org.jgroups.protocols.relay;

import java.util.Objects;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Route which forwards all messages to a given site via a given route.<p>
 * Example: if site HF is reachable via NET1, and NET2 is not directly connected to HF, it needs to have a
 * ForwardConfig with to="HF" and gateway="NET1". This means that a message to site HF will be forwarded to NET1,
 * which then forwards it to HF.<p>
 * Note that 'to' can be a regular expression and {@link #matches(String)} can be used to match a given site against it.
 * @author Bela Ban
 * @since  5.2.15
 */
public class ForwardingRoute implements Comparable<ForwardingRoute> {
    protected String  to;      // target site
    protected String  gateway; // actual site to use for routing (routes.get(gateway); has to point to an existing route
    protected Pattern p;

    public ForwardingRoute(String to, String gateway) {
        this.to=Objects.requireNonNull(to);
        this.gateway=Objects.requireNonNull(gateway);
        p=Pattern.compile(to);
    }

    public String          to()                 {return to;}
    public ForwardingRoute to(String to)        {this.to=Objects.requireNonNull(to); p=Pattern.compile(to); return this;}
    public String          gateway()            {return gateway;}
    public ForwardingRoute gateway(String gw)   {this.gateway=Objects.requireNonNull(gw); return this;}
    public boolean         matches(String site) {Matcher m=p.matcher(site); return m.matches();}

    @Override
    public int compareTo(ForwardingRoute o) {
        if(this == o)
            return 0;
        int rc=gateway.compareTo(o.gateway);
        return rc != 0? rc : to.compareTo(o.to);
    }

    @Override
    public boolean equals(Object obj) {
        return compareTo((ForwardingRoute)obj) == 0;
    }

    @Override
    public String toString() {
        return String.format("to=%s, gw=%s", to, gateway);
    }
}
