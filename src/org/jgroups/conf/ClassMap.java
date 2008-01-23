package org.jgroups.conf;

import org.jgroups.util.Util;


/**
 * Maintains mapping between magic number and class
 *
 * @author Filip Hanik (<a href="mailto:filip@filip.net">filip@filip.net)
 * @author Bela Ban
 * @version 1.0
 */
public class ClassMap {
    private final String  mClassname;
    private final short   mMagicNumber;

    public ClassMap(String clazz, short magicnumber) {
        mClassname=clazz;
        mMagicNumber=magicnumber;
    }

    public int hashCode() {
        return getMagicNumber();
    }

    public String getClassName() {
        return mClassname;
    }

    public short getMagicNumber() {
        return mMagicNumber;
    }


    /**
     * Returns the Class object for this class<BR>
     */
    public Class getClassForMap() throws ClassNotFoundException {
        return Util.loadClass(getClassName(), this.getClass());
    }


    public boolean equals(Object another) {
        if(another instanceof ClassMap) {
            ClassMap obj=(ClassMap)another;
            return getClassName().equals(obj.getClassName());
        }
        else
            return false;
    }


}
