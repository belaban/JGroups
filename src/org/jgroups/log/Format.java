// $Id: Format.java,v 1.1 2003/09/09 01:24:09 belaban Exp $

package org.jgroups.log;


import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * Provides commonly used string formatting methods.
 *
 * @author Jim Menard, <a href="mailto:jimm@io.com">jimm@io.com</a>
 */
public class Format
{

    protected static final String[] LEVEL_STRINGS =
    {
        "DEBUG",
        "TEST",
        "INFO",
        "WARN",
        "ERROR",
        "FATAL"
    };

    /**
     * Return <code>date</code> as a formatted string using the ISO 8601 date
     * format ("yyyy-MM-dd'T'hh:mm:ss,S"). Calls
     * <code>formatTimestamp(Date, String)</code> with a format string of
     * "yyyy-MM-dd'T'hh:mm:ss,S".
     *
     * @param date a <code>Date</code> object
     * @return a formatted string
     * @see #formatTimestamp(Date, String)
     */
    public static String formatTimestamp(Date date) {
        return formatTimestamp(date, null);
    }

    /**
     * Return <code>date</code> as a formatted string. The <code>format</code>
     * string is that used by {@link java.text.SimpleDateFormat}.
     * <p>
     * If <code>format</code> is <code>null</code>, then the format string
     * "yyyy-MM-dd'T'hh:mm:ss,S" is used to generate an ISO 8601 date format.
     *
     * @param date a <code>Date</code> object
     * @param format a format string
     * @return a formatted string
     */
    public static String formatTimestamp(Date date, String format) {
        if (format == null)
        {
            // format = "yyyy-MM-dd'T'hh:mm:ss,S";
            format = "EEE MMM d HH:mm:ss z yyyy";  // changed by bela
        }
        return new SimpleDateFormat(format).format(date);
    }

    /**
     * Given a <code>Trace</code> level value, return a string describing the
     * level. For example, a value of <code>Trace.DEBUG</code> returns the
     * string "DEBUG". Unknown trace levels return the string "<unknown>".
     *
     * @param level a <code>Trace</code> level value (for example,
     * <code>Trace.DEBUG</code> or <code>Trace.FATAL</code>)
     * @return a string describing the level
     */
    public static String levelToString(int level) {
        try {
            return LEVEL_STRINGS[level];
        }
        catch (IndexOutOfBoundsException e) {
            return "<unknown>";
        }
    }

}
