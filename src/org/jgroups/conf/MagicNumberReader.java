
package org.jgroups.conf;

/**
 * Reads and maintains mapping between magic numbers and classes
 * @author Filip Hanik (<a href="mailto:filip@filip.net">filip@filip.net)
 * @author Bela Ban
 * @version $Id: MagicNumberReader.java,v 1.18 2009/09/21 09:57:33 belaban Exp $
 */

import org.jgroups.logging.Log;
import org.jgroups.logging.LogFactory;
import org.jgroups.ChannelException;
import org.jgroups.util.Util;
import org.w3c.dom.Document;
import org.w3c.dom.NamedNodeMap;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;

public class MagicNumberReader {
    public static final String MAGIC_NUMBER_FILE="jg-magic-map.xml";

    public String mMagicNumberFile=MAGIC_NUMBER_FILE;

    protected static final Log log=LogFactory.getLog(MagicNumberReader.class);

    public void setFilename(String file) {
        mMagicNumberFile=file;
    }

    /**
     * try to read the magic number configuration file as a Resource form the classpath using getResourceAsStream
     * if this fails this method tries to read the configuration file from mMagicNumberFile using a FileInputStream (not in classpath but somewhere else in the disk)
     *
     * @return an array of ClassMap objects that where parsed from the file (if found) or an empty array if file not found or had en exception
     */
    public ClassMap[] readMagicNumberMapping() throws Exception {
        InputStream stream;
        try {
            stream=Util.getResourceAsStream(mMagicNumberFile, this.getClass());
            // try to load the map from file even if it is not a Resource in the class path
            if(stream == null) {
                if(log.isTraceEnabled())
                    log.trace("Could not read " + mMagicNumberFile + " as Resource from the CLASSPATH, will try to read it from file.");
                stream=new FileInputStream(mMagicNumberFile);
            }
        }
        catch(Exception x) {
            throw new ChannelException(mMagicNumberFile + " not found. Please make sure it is on the classpath.", x);
        }
        return parse(stream);
    }

    protected static ClassMap[] parse(InputStream stream) throws Exception {
        DocumentBuilderFactory factory=DocumentBuilderFactory.newInstance();
        factory.setValidating(false); //for now
        DocumentBuilder builder=factory.newDocumentBuilder();
        Document document=builder.parse(stream);
        NodeList class_list=document.getElementsByTagName("class");
        java.util.Vector<ClassMap> v=new java.util.Vector<ClassMap>();
        for(int i=0; i < class_list.getLength(); i++) {
            if(class_list.item(i).getNodeType() == Node.ELEMENT_NODE) {
                v.addElement(parseClassData(class_list.item(i)));
            }
        }
        ClassMap[] data=new ClassMap[v.size()];
        v.copyInto(data);
        return data;
    }

    protected static ClassMap parseClassData(Node protocol) throws java.io.IOException {
        try {
            protocol.normalize();
            NamedNodeMap attrs=protocol.getAttributes();
            String clazzname;
            String magicnumber;

            magicnumber=attrs.getNamedItem("id").getNodeValue();
            clazzname=attrs.getNamedItem("name").getNodeValue();
            return new ClassMap(clazzname, Short.valueOf(magicnumber));
        }
        catch(Exception x) {
            IOException tmp=new IOException();
            tmp.initCause(x);
            throw tmp;
        }
    }


}
