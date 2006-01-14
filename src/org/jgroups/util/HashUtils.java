package org.jgroups.util;

import java.util.*;
import java.io.*;
import java.security.*;
/**
 * Helper class for performing some common hashing methods
 *
 * @author Chris Mills
 */
public class HashUtils {
    /**
     * Used to convert a byte array in to a java.lang.String object
     * @param bytes the bytes to be converted
     * @return the String representation
     */
    private static String getString(byte[] bytes) {
        StringBuffer sb = new StringBuffer();
        for (int i = 0; i < bytes.length; i++) {
            byte b = bytes[i];
            sb.append(0x00FF & b);
            if (i + 1 < bytes.length) {
                sb.append("-");
            }
        }
        return sb.toString();
    }
    /**
     * Used to convert a java.lang.String in to a byte array
     * @param str the String to be converted
     * @return the byte array representation of the passed in String
     */
    private static byte[] getBytes(String str) {
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        StringTokenizer st = new StringTokenizer(str, "-", false);
        while (st.hasMoreTokens()) {
            int i = Integer.parseInt(st.nextToken());
            bos.write((byte) i);
        }
        return bos.toByteArray();
    }

    /**
     * Converts a java.lang.String in to a MD5 hashed String
     * @param source the source String
     * @return the MD5 hashed version of the string
     */
    public static String md5(String source) {
        try {
            MessageDigest md = MessageDigest.getInstance("MD5");
            byte[] bytes = md.digest(source.getBytes());
            return getString(bytes);
        } catch (Exception e) {
            return null;
        }
    }
    /**
     * Converts a java.lang.String in to a SHA hashed String
     * @param source the source String
     * @return the MD5 hashed version of the string
     */
    public static String sha(String source) {
        try {
            MessageDigest md = MessageDigest.getInstance("SHA");
            byte[] bytes = md.digest(source.getBytes());
            return getString(bytes);
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }
    }
}
