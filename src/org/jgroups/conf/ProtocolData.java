
package org.jgroups.conf;

/**
 * Data holder for protocol
 * @author Filip Hanik (<a href="mailto:filip@filip.net">filip@filip.net)
 * @author Bela Ban
 * @version $Id: ProtocolData.java,v 1.8 2007/12/03 13:17:08 belaban Exp $
 */

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

public class ProtocolData {
    /** Map<String,ProtocolParameter> of property keys and values */
    private final Map<String,ProtocolParameter> mParameters=new HashMap<String,ProtocolParameter>();
    private final String mProtocolName;
    private final String mDescription;
    private final String mClassName;
    private boolean mIsOverRide=false;

    public ProtocolData(String protocolName,
                        String description,
                        String className,
                        ProtocolParameter[] params) {
        mProtocolName=protocolName;
        mDescription=description;
        mClassName=className;
        if(params != null) {
            for(int i=0; i < params.length; i++) {
                mParameters.put(params[i].getName(), params[i]);
            }
        }
    }

    public ProtocolData(String overRideName, ProtocolParameter[] params) {
        this(overRideName, null, null, params);
        mIsOverRide=true;

    }

    public String getClassName() {
        return mClassName;
    }

    public String getProtocolName() {
        return mProtocolName;
    }

    public String getDescription() {
        return mDescription;
    }

    public Map<String,ProtocolParameter> getParameters() {
        return mParameters;
    }

    public boolean isOverride() {
        return mIsOverRide;
    }

    public ProtocolParameter[] getParametersAsArray() {
        ProtocolParameter[] result=new ProtocolParameter[mParameters.size()];
        Iterator it=mParameters.keySet().iterator();
        for(int i=0; i < result.length; i++) {
            String key=(String)it.next();
            result[i]=mParameters.get(key);
        }
        return result;
    }


    public void override(ProtocolParameter[] params) {
        for(int i=0; i < params.length; i++)
            mParameters.put(params[i].getName(), params[i]);
    }

    public String getProtocolString(boolean new_format) {
        return new_format? getProtocolStringNewXml() : getProtocolString();
    }

    public String getProtocolString() {
        StringBuilder buf=new StringBuilder(mClassName);
        if(!mParameters.isEmpty()) {
            buf.append('(');
            Iterator i=mParameters.keySet().iterator();
            while(i.hasNext()) {
                String key=(String)i.next();
                ProtocolParameter param=mParameters.get(key);
                buf.append(param.getParameterString());
                if(i.hasNext()) buf.append(';');
            }
            buf.append(')');
        }
        return buf.toString();
    }

    public String getProtocolStringNewXml() {
        StringBuilder buf=new StringBuilder(mClassName + ' ');
        if(!mParameters.isEmpty()) {
            Iterator i=mParameters.keySet().iterator();
            while(i.hasNext()) {
                String key=(String)i.next();
                ProtocolParameter param=mParameters.get(key);
                buf.append(param.getParameterStringXml());
                if(i.hasNext()) buf.append(' ');
            }
        }
        return buf.toString();
    }

    public int hashCode() {
        return mProtocolName.hashCode();
    }

    public boolean equals(Object another) {
        return another instanceof ProtocolData && getProtocolName().equals(((ProtocolData)another).getProtocolName());
    }


}
