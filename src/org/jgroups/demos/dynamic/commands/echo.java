package org.jgroups.demos.dynamic.commands;

import org.jgroups.demos.dynamic.Command;

/**
 * @author bela
 * @since x.y
 */
public class echo extends Command {
    public Object invoke(Object[] args) throws Exception {
        StringBuilder sb=new StringBuilder("hello " + args[0] + " !");
        if(args.length >= 2)
            sb.append("Oh, I see you are from " + args[1]);
        return sb.toString();
    }

    public String help() {
        return "name [town]";
    }
}
