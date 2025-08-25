package org.jgroups.util;

import java.util.Arrays;

/**
 * Simple string implemented as a byte[] array. Each character's higher 8 bits are truncated and
 * only the lower 8 bits are stored. AsciiString is mutable for efficiency reasons, but the chars array should never
 * be changed !
 * @author Bela Ban
 * @since  3.5
 */
public record AsciiString(byte[] val) implements Comparable<AsciiString> {

    public AsciiString(String str) {
        this(fromString(str));
    }

    public AsciiString(AsciiString str) {
        this(str.val);
    }

    public AsciiString(byte[] val) {
        this.val=val != null? val : new byte[]{}; // mutable, used only for creation
    }

    public int length() {
        return val.length;
    }

    public int compareTo(AsciiString str) {
        if(str == null) return 1;
        if(this.val.hashCode() == str.val.hashCode())
            return 0;

        int len1=val.length;
        int len2=str.val.length;
        int lim=Math.min(len1, len2);

        int k = 0;
        while (k < lim) {
            byte c1 =val[k];
            byte c2 =str.val[k];
            if (c1 != c2)
                return c1 > c2? 1 : -1;
            k++;
        }
        return Integer.compare(len1, len2);
    }

    public boolean equals(Object obj) {
        return obj instanceof AsciiString as && equals(as.val);
    }

    public boolean equals(byte[] other) {
        return Arrays.equals(val, other);
    }

    public int hashCode() {
        int h=0;
        if(val != null)
            for(int i=0; i < val.length; i++)
                h=31 * h + val[i];
        return h;
    }

    public String toString() {
        return new String(val);
    }

    private static byte[] fromString(String str) {
        int length=str != null? str.length() : 0;
        byte[] b=new byte[length];
        for(int i=0; i < length; i++)
            b[i]=(byte)str.charAt(i);
        return b;
    }

}
