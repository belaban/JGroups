// $Id: ProtocolParameter.java,v 1.2 2004/04/24 11:19:27 belaban Exp $

package org.jgroups.conf;

/**
 * Data holder for protocol data
 *
 * @author Filip Hanik (<a href="mailto:filip@filip.net">filip@filip.net)
 * @version 1.0
 */

public class ProtocolParameter {

    private String mParameterName;
    private Object mParameterValue;

    public ProtocolParameter(String parameterName,
                             Object parameterValue) {
        mParameterName=parameterName;
        mParameterValue=parameterValue;
    }

    public String getName() {
        return mParameterName;
    }

    public Object getValue() {
        return mParameterValue;
    }

    public int hashCode() {
        if(mParameterName != null)
            return mParameterName.hashCode();
        else
            return -1;
    }

    public boolean equals(Object another) {
        if(another instanceof ProtocolParameter)
            return getName().equals(((ProtocolParameter)another).getName());
        else
            return false;
    }

    public String getParameterString() {
        StringBuffer buf=new StringBuffer(mParameterName);
        if(mParameterValue != null)
            buf.append("=").append(mParameterValue.toString());
        return buf.toString();
    }

    public String getParameterStringXml() {
        StringBuffer buf=new StringBuffer(mParameterName);
        if(mParameterValue != null)
            buf.append("=\"").append(mParameterValue.toString()).append("\"");
        return buf.toString();
    }
}
