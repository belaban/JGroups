package org.jgroups.util;

/**
 * Simple string implemented as a byte[] array. Each character's higher 8 bits are truncated and
 * only the lower 8 bits are stored. AsciiString is mutable for efficiency reasons, but the chars array should never
 * be changed !
 * @author Bela Ban
 * @since  3.5
 */
public class AsciiString implements Comparable<AsciiString> {
    protected final byte[] chars;

    public AsciiString() {
        chars=new byte[]{};
    }

    public AsciiString(String str) {
        int length=str.length();
        this.chars=new byte[length];
        for(int i=0; i < length; i++)
            chars[i]=(byte)str.charAt(i);
    }

    public AsciiString(AsciiString str) {
        this.chars=str.chars;
    }

    public AsciiString(byte[] chars) {
        this.chars=chars; // mutable, used only for creation
    }

    public AsciiString(int length) {
        this.chars=new byte[length];
    }

    public byte[] chars() {return chars;} // mutable

    public int length() {
        return chars.length;
    }

    public int compareTo(AsciiString str) {
        if(str == null) return 1;
        if(chars().hashCode() == str.chars.hashCode())
            return 0;

        int len1=chars.length;
        int len2=str.chars.length;
        int lim=Math.min(len1, len2);
        byte[] v1=chars;
        byte[] v2=str.chars;

        int k = 0;
        while (k < lim) {
            byte c1 =v1[k];
            byte c2 =v2[k];
            if (c1 != c2)
                return c1 > c2? 1 : -1;
            k++;
        }
        return len1 > len2? 1 : len1 < len2? -1 : 0;
    }



    public boolean equals(Object obj) {
        return obj instanceof AsciiString && compareTo((AsciiString)obj) == 0;
    }

    public int hashCode() {
        int h=0;
        for(int i=0; i < chars.length; i++)
            h=31 * h + chars[i];
        return h;
    }

    public String toString() {
        return new String(chars);
    }




}
