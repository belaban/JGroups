// $Id: ProtocolData.java,v 1.1 2003/09/09 01:24:08 belaban Exp $

package org.jgroups.conf;

/**
 * Data holder for protocol
 * @author Filip Hanik (<a href="mailto:filip@filip.net">filip@filip.net)
 * @version 1.0
 */
import java.util.Iterator;
import java.util.HashMap;
public class ProtocolData
{
    private HashMap mParameters = new HashMap(); 
    private String mProtocolName;
    private String mDescription;
    private String mClassName;
    private boolean mIsOverRide = false;
    
    public ProtocolData(String protocolName, 
                        String description, 
                        String className, 
                        ProtocolParameter[] params)
    {
        mProtocolName = protocolName;
        mDescription  = description;
        mClassName    = className;
        if ( params != null )  
            for ( int i=0; i<params.length; i++)
            {
                mParameters.put(params[i].getName(),params[i]);
            }//for
    }
    
    public ProtocolData(String overRideName, 
                        ProtocolParameter[] params)
    {
        this(overRideName,null,null,params);
        mIsOverRide = true;
        
    }
    
    public String getClassName() { return mClassName; }
    public String getProtocolName() { return mProtocolName; }
    public String getDescription() { return mDescription; }
    public HashMap getParameters() { return mParameters; }
    public boolean isOverride() { return mIsOverRide; }
    
    public ProtocolParameter[] getParametersAsArray() 
    { 
        ProtocolParameter[] result = new ProtocolParameter[mParameters.size()];
        Iterator it = mParameters.keySet().iterator();
        for ( int i=0; i<result.length; i++ )
        {
            String key = (String)it.next();
            result[i] = (ProtocolParameter)mParameters.get(key);
        }
        return result;
    }
    
    
    public void override(ProtocolParameter[] params)
    {
        for ( int i=0; i<params.length; i++ )
            mParameters.put(params[i].getName(),params[i]);
    }
    
    public String getProtocolString()
    {
        StringBuffer buf= new StringBuffer(mClassName);
        if ( mParameters.size() > 0)
        {
            buf.append("(");
            Iterator i = mParameters.keySet().iterator();
            while ( i.hasNext() )
            {
                String key = (String)i.next();
                ProtocolParameter param = (ProtocolParameter)mParameters.get(key);
                buf.append(param.getParameterString());
                if ( i.hasNext() ) buf.append(";"); 
            }//while       
            buf.append(")");
        }
        return buf.toString();
    }
    
    public int hashCode()
    {
        return mProtocolName.hashCode();
    }
    
    public boolean equals(Object another)
    {
        if ( another instanceof ProtocolData )
           return getProtocolName().equals(((ProtocolData)another).getProtocolName()); 
        else
            return false;
    }
    
    
}
