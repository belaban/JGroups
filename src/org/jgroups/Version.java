


package org.jgroups;

/**
 * Holds version information for JGroups.
 */
public class Version {
	
	/**
	 * Value is {@value}.
	 */
    public static final String description="2.2.9 alpha";
	/**
	 * Value is {@value}.
	 */
    public static final short version=229;
	/**
	 * Value is {@value}.
	 */
    public static final String cvs="$Id: Version.java,v 1.22 2005/07/17 11:38:05 chrislott Exp $";

    /**
     * Prints the value of the description and cvs fields to System.out.
     * @param args
     */
    public static void main(String[] args) {
        System.out.println("\nVersion: \t" + description);
        System.out.println("CVS: \t\t" + cvs);
        System.out.println("History: \t(see doc/history.txt for details)\n");
    }

    /**
     * Returns the catenation of the description and cvs fields.
     * @return String with description
     */
    public static String printDescription() {
        return "JGroups " + description + "[ " + cvs + "]";
    }

    /**
     * Returns the version field as a String.
     * @return String with version
     */
    public static String printVersion() {
        return new Short(version).toString();
    }

    /**
     * Compares the specified version number against the current version number.
     * @param v short
     * @return Result of == operator.
     */
    public static boolean compareTo(short v) {
        return version == v;
    }
}
