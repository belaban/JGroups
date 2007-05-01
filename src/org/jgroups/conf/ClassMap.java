// $Id: ClassMap.java,v 1.5 2007/05/01 09:15:18 belaban Exp $

package org.jgroups.conf;

import org.jgroups.util.Util;


/**
 * Maintains mapping between magic number and class
 *
 * @author Filip Hanik (<a href="mailto:filip@filip.net">filip@filip.net)
 * @version 1.0
 */
public class ClassMap {
    private final String  mClassname;
    private final String  mDescription;
    private final boolean mPreload;
    private final short    mMagicNumber;

    public ClassMap(String clazz,
                    String desc,
                    boolean preload,
                    short magicnumber) {
        mClassname=clazz;
        mDescription=desc;
        mPreload=preload;
        mMagicNumber=magicnumber;
    }

    public int hashCode() {
        return getMagicNumber();
    }

    public String getClassName() {
        return mClassname;
    }

    public String getDescription() {
        return mDescription;
    }

    public boolean getPreload() {
        return mPreload;
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
