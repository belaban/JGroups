package org.jgroups.demos.dynamic.commands;

import org.jgroups.Message;
import org.jgroups.demos.dynamic.Arguments;
import org.jgroups.demos.dynamic.Command;
import org.jgroups.demos.dynamic.DTest;
import org.jgroups.demos.dynamic.DTestHeader;

/**
 * Invokes a command across the entire cluster
 * @author Bela Ban
 * @since 3.1
 */
public class all extends Command {
    public Object invoke(Object[] args) throws Exception {
        if(args == null || args.length < 1)
            throw new IllegalArgumentException("arguments are incorrect");

        String command_name=(String)args[0];
        Object[] tmp=args.length > 1? new Object[args.length -1] : null;
        if(args.length > 1)
            System.arraycopy(args, 1, tmp, 0, args.length - 1);

        Arguments arguments=new Arguments(command_name, tmp);
        Message msg=new Message(null, null, arguments);
        msg.setFlag(Message.Flag.RSVP);
        msg.putHeader(DTest.ID, new DTestHeader(DTestHeader.REQ));
        test.getChannel().send(msg);
        return null;
    }

    public String help() {
        return "command [arg]*";
    }
}
