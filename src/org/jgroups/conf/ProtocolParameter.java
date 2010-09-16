
package org.jgroups.conf;

/**
 * Data holder for protocol data
 *
 * @author Filip Hanik (<a href="mailto:filip@filip.net">filip@filip.net)
 * @author Bela Ban
 * @version $Id: ProtocolParameter.java,v 1.7 2010/09/16 07:13:53 belaban Exp $
 */

public class ProtocolParameter {
    private final String mParameterName;
    private String mParameterValue;

    public ProtocolParameter(String parameterName, String parameterValue) {
        mParameterName=parameterName;
        mParameterValue=parameterValue;
    }

    public String getName() {
        return mParameterName;
    }

    public String getValue() {
        return mParameterValue;
    }

    public void setValue(String replacement) {
        mParameterValue=replacement;
    }

    public int hashCode() {
        if(mParameterName != null)
            return mParameterName.hashCode();
        else
            return -1;
    }

    public boolean equals(Object another) {
        return another instanceof ProtocolParameter && getName().equals(((ProtocolParameter)another).getName());
    }

 


}
