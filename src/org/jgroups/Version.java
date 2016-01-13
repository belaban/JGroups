


package org.jgroups;

import org.jgroups.annotations.Immutable;
import org.jgroups.util.Util;

import java.io.FileNotFoundException;
import java.io.InputStream;
import java.util.Properties;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * We're using the scheme described at http://www.jboss.com/index.html?module=bb&op=viewtopic&t=77231
 * for major, minor and micro version numbers. We have 5 bits for major and minor version numbers each and
 * 6 bits for the micro version.
 * This gives:
 * X = 0-31 for major versions
 * Y = 0-31 for minor versions
 * Z = 0-63 for micro versions
 *
 * @author Bela Ban
 * Holds version information for JGroups.
 */
@Immutable
public class Version {
    public static final short    MAJOR;
    public static final short    MINOR;
    public static final short    MICRO;
    public static final String   DESCRIPTION;

    public static final short    VERSION;
    public static final String   STRING_VERSION;

    private static final int     MAJOR_SHIFT = 11;
    private static final int     MINOR_SHIFT = 6;
    private static final int     MAJOR_MASK  = 0x00f800; // 1111100000000000 bit mask
    private static final int     MINOR_MASK  = 0x0007c0; //      11111000000 bit mask
    private static final int     MICRO_MASK  = 0x00003f; //           111111 bit mask

    public static final String  VERSION_FILE="JGROUPS_VERSION.properties";
    public static final String  VERSION_PROPERTY = "jgroups.version";
    private static final Pattern VERSION_REGEXP = Pattern.compile("((\\d+)\\.(\\d+)\\.(\\d+).*)");

    

    static {
        Properties properties=new Properties();
        String value=null;
        InputStream manifestAsStream=null;
        try {
            manifestAsStream=Util.getResourceAsStream(VERSION_FILE, Version.class);
            if(manifestAsStream == null)
                throw new FileNotFoundException(VERSION_FILE);
            properties.load(manifestAsStream);
            value=properties.getProperty(VERSION_PROPERTY);
            if(value == null)
                throw new Exception("value for " + VERSION_PROPERTY + " not found in " + VERSION_FILE);
        } catch(Exception e) {
            throw new IllegalStateException("Could not initialize version", e);
        } finally {
            Util.close(manifestAsStream);
        }

        Matcher versionMatcher = VERSION_REGEXP.matcher(value);
        versionMatcher.find();

        DESCRIPTION = value;
        MAJOR = Short.parseShort(versionMatcher.group(2));
        MINOR = Short.parseShort(versionMatcher.group(3));
        MICRO = Short.parseShort(versionMatcher.group(4));
        VERSION =encode(MAJOR, MINOR, MICRO);
        STRING_VERSION =print(VERSION);
    }


    /**
     * Prints the value of the description and cvs fields to System.out.
     * @param args
     */
    public static void main(String[] args) {
        System.out.println("\nVersion:      " + DESCRIPTION);
    }


    /**
     * Returns the catenation of the description and cvs fields.
     * @return String with description
     */
    public static String printDescription() {
        return "JGroups " + DESCRIPTION;
    }

    /**
     * Returns the version field as a String.
     * @return String with version
     */
    public static String printVersion() {
        return STRING_VERSION;
    }


    /**
     * Compares the specified version number against the current version number.
     * @param v short
     * @return Result of == operator.
     */
    public static boolean isSame(short v) {
        return VERSION == v;
    }

    /** Method copied from http://www.jboss.com/index.html?module=bb&op=viewtopic&t=77231 */
    public static short encode(int major, int minor, int micro) {
        return (short)((major << MAJOR_SHIFT) + (minor << MINOR_SHIFT) + micro);
    }

    /** Method copied from http://www.jboss.com/index.html?module=bb&op=viewtopic&t=77231 */
    public static String print(short version) {
        int major=(version & MAJOR_MASK) >> MAJOR_SHIFT;
        int minor=(version & MINOR_MASK) >> MINOR_SHIFT;
        int micro=(version & MICRO_MASK);
        return major + "." + minor + "." + micro;
    }


    public static short[] decode(short version) {
        short major=(short)((version & MAJOR_MASK) >> MAJOR_SHIFT);
        short minor=(short)((version & MINOR_MASK) >> MINOR_SHIFT);
        short micro=(short)(version & MICRO_MASK);
        return new short[]{major, minor, micro};
    }

    /**
     * Checks whether ver is binary compatible with the current version. The rule for binary compatibility is that
     * the major and minor versions have to match, whereas micro versions can differ.
     * @param ver
     * @return
     */
    public static boolean isBinaryCompatible(short ver) {
        if(VERSION == ver)
            return true;
        short tmp_major=(short)((ver & MAJOR_MASK) >> MAJOR_SHIFT);
        short tmp_minor=(short)((ver & MINOR_MASK) >> MINOR_SHIFT);
        return MAJOR == tmp_major && MINOR == tmp_minor;
    }


    public static boolean isBinaryCompatible(short ver1, short ver2) {
        if(ver1 == ver2)
            return true;
        short[] tmp=decode(ver1);
        short tmp_major=tmp[0], tmp_minor=tmp[1];
        tmp=decode(ver2);
        short tmp_major2=tmp[0], tmp_minor2=tmp[1];
        return tmp_major == tmp_major2 && tmp_minor == tmp_minor2;
    }

}
