// $Id: ClassMap.java,v 1.1.1.1 2003/09/09 01:24:08 belaban Exp $

package org.jgroups.conf;



/**
 * Maintains mapping between magic number and class
 * @author Filip Hanik (<a href="mailto:filip@filip.net">filip@filip.net)
 * @version 1.0
 */
public class ClassMap
{
    private String mClassname;
    private String mDescription;
    private boolean mPreload;
    private int mMagicNumber;
    
    public ClassMap(String clazz,
                    String desc,
                    boolean preload,
                    int magicnumber)
    {
        mClassname = clazz;
        mDescription = desc;
        mPreload = preload;
        mMagicNumber = magicnumber;
    }
    
    public int hashCode() { return getMagicNumber(); }
    public String getClassName() { return mClassname; }
    public String getDescription() { return mDescription; }
    public boolean getPreload() { return mPreload; }
    public int getMagicNumber() { return mMagicNumber; }
    

    //public Class getClassForMap() throws ClassNotFoundException
    // { 
    //     return getClass().getClassLoader().loadClass(getClassName());
    // }


    /**
     * Returns the Class object for this class<BR>
     */
    public Class getClassForMap() throws ClassNotFoundException
    {
        try
            {
                return getClass().getClassLoader().loadClass(getClassName());
            }
        catch ( Exception x )
            {
                return
                    Thread.currentThread().getContextClassLoader().loadClass(getClassName());
            }
    }
    


    
    public boolean equals(Object another)
    {
        if ( another instanceof ClassMap )
        {
            ClassMap obj = (ClassMap)another;
            return getClassName().equals(obj.getClassName());
        }
        else
            return false;
    }
    
    
}
