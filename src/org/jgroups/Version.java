


package org.jgroups;


public class Version {
    public static final String description="2.2.9 alpha";
    public static final short version=229;
    public static final String cvs="$Id: Version.java,v 1.21 2005/06/15 21:08:01 belaban Exp $";

    public static void main(String[] args) {
        System.out.println("\nVersion: \t" + description);
        System.out.println("CVS: \t\t" + cvs);
        System.out.println("History: \t(see doc/history.txt for details)\n");
    }


    public static String printDescription() {
        return "JGroups " + description + "[ " + cvs + "]";
    }


    public static String printVersion() {
        return new Short(version).toString();
    }

    public static boolean compareTo(short v) {
        return version == v;
    }
}
