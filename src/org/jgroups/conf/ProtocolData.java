
package org.jgroups.conf;

/**
 * Data holder for protocol
 * @author Filip Hanik (<a href="mailto:filip@filip.net">filip@filip.net)
 * @author Bela Ban
 * @version $Id: ProtocolData.java,v 1.15 2010/09/16 07:42:12 belaban Exp $
 */

import java.util.HashMap;
import java.util.Map;




public class ProtocolData {
    /** Map<String,ProtocolParameter> of property keys and values */
    private final Map<String,String>    mParameters=new HashMap<String,String>();
    private final String                mProtocolName;
    private final String                mClassName;


    public ProtocolData(String protocolName, String className, Map<String,String> params) {
        mProtocolName=protocolName;
        mClassName=className;
        if(params != null) {
            mParameters.putAll(params);
        }
    }

    public String getClassName() {
        return mClassName;
    }

    public String getProtocolName() {
        return mProtocolName;
    }


    public Map<String,String> getParameters() {
        return mParameters;
    }


    public String getProtocolString(boolean new_format) {
        return new_format? getProtocolStringNewXml() : getProtocolString();
    }

    public String getProtocolString() {
        StringBuilder buf=new StringBuilder(mClassName);
        if(!mParameters.isEmpty()) {
            boolean first=true;
            buf.append('(');
            for(Map.Entry<String,String> entry: mParameters.entrySet()) {
                String key=entry.getKey();
                String val=entry.getValue();
                if(first)
                    first=false;
                else
                    buf.append(';');
                buf.append(getParameterString(key, val));
            }
            buf.append(')');
        }
        return buf.toString();
    }
    

    public String getProtocolStringNewXml() {
        StringBuilder buf=new StringBuilder(mClassName + ' ');
        if(!mParameters.isEmpty()) {
            boolean first=true;
            for(Map.Entry<String,String> entry: mParameters.entrySet()) {
                String key=entry.getKey();
                String val=entry.getValue();
                if(first)
                    first=false;
                else
                    buf.append(' ');
                buf.append(getParameterStringXml(key, val));
            }
        }
        return buf.toString();
    }

    protected static String getParameterString(String name, String value) {
        StringBuilder buf=new StringBuilder(name);
        if(value != null)
            buf.append('=').append(value);
        return buf.toString();
    }

    protected static String getParameterStringXml(String name, String val) {
        StringBuilder buf=new StringBuilder(name);
        if(val != null)
            buf.append("=\"").append(val).append('\"');
        return buf.toString();
    }


    public int hashCode() {
        return mProtocolName.hashCode();
    }

    public boolean equals(Object another) {
        return another instanceof ProtocolData && getProtocolName().equals(((ProtocolData)another).getProtocolName());
    }


}
