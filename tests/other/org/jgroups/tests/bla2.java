package org.jgroups.tests;

/**
 * @author Bela Ban
 * @version $Id$
 */
public class bla2 {

    static enum Destination {
        MESSAGES("/messages"),
        CLIENT_JOINED("/client-joined");

        protected final String name;

        Destination(String name) {
            this.name=name;
        }

        public String getName() {
            return name;
        }
    }


    public static void main(String[] args) {
        Destination dest=Destination.valueOf(args[0]);

        System.out.println("dest = " + dest.getName() + ", " + dest.name());


    }
}
