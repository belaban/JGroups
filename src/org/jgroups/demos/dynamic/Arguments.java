package org.jgroups.demos.dynamic;

import org.jgroups.util.Streamable;
import org.jgroups.util.Util;

import java.io.DataInput;
import java.io.DataOutput;
import java.util.List;

/**
 * Encapsulates the arguments submitted against a {@link Command}.
 * @author Bela Ban
 * @since 3.1
 */
public class Arguments implements Streamable {

    protected String command_name;

    /** A list of arguments that are passed to {@link Command#invoke(Object[])} */
    protected Object[]    args;



    public Arguments() {
    }


    public Arguments(String command_name, Object ... args) {
        this.command_name=command_name;
        this.args=args;
    }


    public Arguments(String command_name, List<Object> args) {
        this.command_name=command_name;
        if(args == null || args.isEmpty())
            return;
        this.args=new Object[args.size()];
        for(int i=0; i < this.args.length; i++)
            this.args[i]=args.get(i);
    }

    public String getCommandName() {
        return command_name;
    }

    public Object[] getArguments() {
        return args;
    }

    public void writeTo(DataOutput out) throws Exception {
        Util.writeString(command_name, out);
        if(args == null || args.length == 0) {
            out.writeShort(0);
            return;
        }
        out.writeShort(args.length);
        for(Object arg: args)
            Util.writeObject(arg, out);
    }

    public void readFrom(DataInput in) throws Exception {
        command_name=Util.readString(in);
        short num_args=in.readShort();
        if(num_args == 0)
            return;
        args=new Object[num_args];
        for(int i=0; i < num_args; i++) {
            Object arg=Util.readObject(in);
            args[i]=arg;
        }
    }

    public String toString() {
        return command_name + " " + (args != null? args: "");
    }
}
