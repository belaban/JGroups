package org.jgroups.admin;

import org.jgroups.Address;
import org.jgroups.ObjectMessage;

import java.util.Objects;

/**
 * Sends a single command
 * @author Bela Ban
 * @since  5.6.0
 */
public class Phase {
    protected AdminHandler admin;
    protected String       key;
    protected String       name;
    protected Address      dest;
    protected String       prepare;
    protected long         timeout;

    public Phase(AdminHandler admin, String key, Address dest, String name, String prepare, long timeout) {
        this.admin=admin;
        this.key=key;
        this.dest=dest;
        this.name=name;
        this.prepare=Objects.requireNonNull(prepare);
        this.timeout=timeout;
    }

    public AdminHandler admin()               {return admin;}
    public void         admin(AdminHandler a) {this.admin=a;}
    public String       key()                 {return key;}
    public void         key(String key)       {this.key=key;}
    public String       name()                {return name;}
    public void         name(String name)     {this.name=name;}
    public Address      dest()                {return dest;}
    public void         dest(Address dest)    {this.dest=dest;}
    public String       prepare()             {return prepare;}
    public void         prepare(String p)     {this.prepare=p;}
    public long         timeout()             {return timeout;}
    public void         timeout(long t)       {this.timeout=t;}


    public boolean execute() {
        admin.sendRequest(key, new ObjectMessage(dest, prepare));
        return true;
    }

    public void performRollback() {
    }

    public void handleResponse(Address sender, String rsp) {
    }


}
