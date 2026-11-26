package org.jgroups.protocols.relay;

import org.jgroups.Address;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.function.Supplier;

/**
 * The default implementation of {@link RouteStatusListener}
 * @author Bela Ban
 * @since  5.3.1
 */
public class DefaultRouteStatusListener implements RouteStatusListener {
    protected final Supplier<Address> addr_getter;
    protected final List<String>      up=new ArrayList<>(), down=new ArrayList<>();
    protected boolean                 verbose;

    public DefaultRouteStatusListener(Supplier<Address> s) {
        this.addr_getter=s;
    }

    public List<String>               up()               {return up;}
    public List<String>               down()             {return down;}
    public DefaultRouteStatusListener verbose(boolean b) {this.verbose=b; return this;}
    public boolean                    verbose()          {return verbose;}
    public Address                    addr()             {return addr_getter != null? addr_getter.get() : null;}

    @Override public synchronized void sitesUp(String... sites) {
        if(verbose)
            System.out.printf("%s: UP(%s)\n", addr(), String.join(",", sites));
        up.addAll(Arrays.asList(sites));
    }

    @Override public synchronized void sitesDown(String... sites) {
        if(verbose)
            System.out.printf("%s: DOWN(%s)\n", addr(), String.join(",", sites));
        down.addAll(Arrays.asList(sites));
    }

    @Override public void sitesUnreachable(String... sites) {
        if(verbose)
            System.out.printf("%s: SITE-UNREACHABLE(%s)\n", addr(), String.join(",", sites));
    }

    @Override
    public void memberUnreachable(Address member) {
        if(verbose)
            System.out.printf("%s: MEMBER-UNREACHABLE: %s\n", addr(), member);
    }

    protected synchronized DefaultRouteStatusListener clear() {up.clear(); down.clear(); return this;}

    @Override
    public String toString() {
        return String.format("down: %s, up: %s", down, up);
    }
}
