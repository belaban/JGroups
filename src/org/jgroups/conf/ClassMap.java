
package org.jgroups.conf;

import org.jgroups.util.Util;


/**
 * Maintains mapping between magic number and class
 *
 * @author Filip Hanik (<a href="mailto:filip@filip.net">filip@filip.net)
 * @author Bela Ban
 * @version $Id: ClassMap.java,v 1.7 2009/09/21 09:57:33 belaban Exp $
 */
public class ClassMap {
    private final String  mClassname;
    private final short   mMagicNumber;

    public ClassMap(String clazz, short magicnumber) {
        mClassname=clazz;
        mMagicNumber=magicnumber;
    }

    public int hashCode() {
        return mMagicNumber;
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
        return Util.loadClass(mClassname, this.getClass());
    }


    public boolean equals(Object another) {
        if(another instanceof ClassMap) {
            ClassMap obj=(ClassMap)another;
            return mClassname.equals(obj.mClassname);
        }
        else
            return false;
    }


}
