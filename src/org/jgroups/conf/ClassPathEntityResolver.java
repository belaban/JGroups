// $Id: ClassPathEntityResolver.java,v 1.2 2005/04/23 12:44:05 belaban Exp $

package org.jgroups.conf;

/**
 * 
 * @author Filip Hanik (<a href="mailto:filip@filip.net">filip@filip.net)
 * @author Bela Ban
 * @version $Id: ClassPathEntityResolver.java,v 1.2 2005/04/23 12:44:05 belaban Exp $
 */

import org.xml.sax.EntityResolver;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;

import java.io.InputStream;
import java.io.IOException;
import java.net.URL;

public class ClassPathEntityResolver implements EntityResolver {
    public String mDefaultJGroupsDTD="jgroups-protocol.dtd";

    public ClassPathEntityResolver() {
    }

    public ClassPathEntityResolver(String dtdName) {
        mDefaultJGroupsDTD=dtdName;
    }

    public InputSource resolveEntity(String publicId, String systemId) throws SAXException, IOException {
        InputSource source=new InputSource(getInputStream(systemId));
        return source;
    }

    protected InputStream getInputStream(String dtdurl)
            throws java.io.IOException {
        String url=dtdurl;
        if(url == null) url=mDefaultJGroupsDTD;
        //1. first try to load the DTD from an actual URL
        try {
            URL inurl=new URL(url);
            return inurl.openStream();
        }
        catch(Exception ignore) {
        }
        //2. then try to load it from the classpath
        
        InputStream stream=Thread.currentThread().getContextClassLoader().getResourceAsStream(url);
        if(stream == null) {
            throw new IOException("Could not locate the DTD with name:[" + url + "] in the classpath.");
        }
        else
            return stream;
    }
}
