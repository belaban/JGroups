
package org.jgroups.conf;

/**
 * Uses XML to configure a protocol stack
 * @author Vladimir Blagojevic
 * @version $Id: XmlConfigurator.java,v 1.23 2009/09/18 10:19:49 belaban Exp $
 */

import org.jgroups.Global;
import org.jgroups.logging.Log;
import org.jgroups.logging.LogFactory;
import org.w3c.dom.*;
import org.jgroups.stack.Configurator;
import org.jgroups.stack.Configurator.ProtocolConfiguration;
import org.jgroups.util.Util;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.util.*;
import java.util.Map.Entry;

public class XmlConfigurator implements ProtocolStackConfigurator {
   
    private static final String JAXP_SCHEMA_LANGUAGE = "http://java.sun.com/xml/jaxp/properties/schemaLanguage";

    private static final String W3C_XML_SCHEMA = "http://www.w3.org/2001/XMLSchema";

    public static final String ATTR_NAME="name";
    public static final String ATTR_VALUE="value";
    public static final String ATTR_INHERIT="inherit";
    public static final String ELMT_PROT_OVERRIDE="protocol-override";
    public static final String ELMT_PROT="protocol";
    public static final String ELMT_PROT_NAME="protocol-name";
    public static final String ELMT_CLASS="class-name";
    public static final String ELMT_DESCRIPTION="description";
    public static final String ELMT_PROT_PARAMS="protocol-params";

    private final ArrayList<ProtocolData> mProtocolStack=new ArrayList<ProtocolData>();
    private final String mStackName;
    protected static final Log log=LogFactory.getLog(XmlConfigurator.class);

    protected XmlConfigurator(String stackName,ProtocolData[] protocols) {
        mStackName=stackName;
        mProtocolStack.addAll(Arrays.asList(protocols));
    }

    protected XmlConfigurator(String stackName) {
        this(stackName, new ProtocolData[0]);
    }

    public static XmlConfigurator getInstance(URL url) throws java.io.IOException {
        return getInstance(url.openStream());
    }

    public static XmlConfigurator getInstance(InputStream stream) throws java.io.IOException {
        return parse(stream);
    }

    public static XmlConfigurator getInstance(Element el) throws java.io.IOException {
        return parse(el);
    }

    /**
     * 
     * @param convert
     *                If false: print old plain output, else print new XML
     *                format
     * @return String with protocol stack in specified format
     */
    public String getProtocolStackString(boolean convert) {
        StringBuilder buf=new StringBuilder();
        Iterator<ProtocolData> it=mProtocolStack.iterator();
        if(convert)
            buf.append("<config>\n");
        while(it.hasNext()) {
            ProtocolData d=it.next();
            if(convert)
                buf.append("    <");
            buf.append(d.getProtocolString(convert));
            if(convert)
                buf.append("/>");
            if(it.hasNext()) {
                if(convert)
                    buf.append('\n');
                else
                    buf.append(':');
            }
        }
        if(convert)
            buf.append("\n</config>");
        return buf.toString();
    }

    public String getProtocolStackString() {
        return getProtocolStackString(false);
    }

    public ProtocolData[] getProtocolStack() {
        return mProtocolStack.toArray(new ProtocolData[mProtocolStack.size()]);
    }

    public String getName() {
        return mStackName;
    }

    public void override(ProtocolData data) throws IOException {
        int index=mProtocolStack.indexOf(data);
        if(index < 0)
            throw new IOException("You can not override a protocol that doesn't exist");
        ProtocolData source=mProtocolStack.get(index);
        source.override(data.getParametersAsArray());
    }

    public void add(ProtocolData data) {
        mProtocolStack.add(data);
    }



    protected static XmlConfigurator parse(InputStream stream) throws java.io.IOException {
        /**
         * CAUTION: crappy code ahead ! I (bela) am not an XML expert, so the code below is pretty
         * amateurish... But it seems to work, and it is executed only on startup, so no perf loss
         * on the critical path. If somebody wants to improve this, please be my guest.
         */
        try {
            DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
            boolean validation = false;
            try {
                String tmp = Util.getProperty(new String[] { Global.XML_VALIDATION }, null, null, false, "false");
                validation = Boolean.valueOf(tmp).booleanValue();
            } catch (Exception e) {
                validation = false;
            }
            if (validation) {
                factory.setValidating(true); 
                factory.setNamespaceAware(validation);
                factory.setAttribute(JAXP_SCHEMA_LANGUAGE, W3C_XML_SCHEMA);
            } else {
                factory.setValidating(false);
            }
            
            DocumentBuilder builder = factory.newDocumentBuilder();
            Document document = builder.parse(stream);

            // The root element of the document should be the "config" element,
            // but the parser(Element) method checks this so a check is not
            // needed here.
            Element configElement = document.getDocumentElement();
            return parse(configElement);
        } catch (Exception x) {
            if (x instanceof java.io.IOException)
                throw (java.io.IOException) x;
            else {
                IOException tmp = new IOException();
                tmp.initCause(x);
                throw tmp;
            }
        }
    }

    protected static XmlConfigurator parse(Element root_element) throws java.io.IOException {
        XmlConfigurator configurator=null;

        /** LinkedList<ProtocolData> */
        LinkedList<ProtocolData> prot_data=new LinkedList<ProtocolData>();

        /**
         * CAUTION: crappy code ahead ! I (bela) am not an XML expert, so the
         * code below is pretty amateurish... But it seems to work, and it is
         * executed only on startup, so no perf loss on the critical path. If
         * somebody wants to improve this, please be my guest.
         */
        try {
            String root_name=root_element.getNodeName();
            if(!"config".equals(root_name.trim().toLowerCase())) {
                log.fatal("XML protocol stack configuration does not start with a '<config>' element; " + "maybe the XML configuration needs to be converted to the new format ?\n"
                          + "use 'java org.jgroups.conf.XmlConfigurator <old XML file> -new_format' to do so");
                throw new IOException("invalid XML configuration");
            }

            NodeList prots=root_element.getChildNodes();
            for(int i=0;i < prots.getLength();i++) {
                Node node=prots.item(i);
                if(node.getNodeType() != Node.ELEMENT_NODE)
                    continue;

                Element tag=(Element)node;
                String protocol=tag.getTagName();
                // System.out.println("protocol: " + protocol);
                LinkedList<ProtocolParameter> tmp=new LinkedList<ProtocolParameter>();

                NamedNodeMap attrs=tag.getAttributes();
                int attrLength=attrs.getLength();
                for(int a=0;a < attrLength;a++) {
                    Attr attr=(Attr)attrs.item(a);
                    String name=attr.getName();
                    String value=attr.getValue();
                    // System.out.println("    name=" + name + ", value=" + value);
                    tmp.add(new ProtocolParameter(name, value));
                }
                ProtocolParameter[] params=new ProtocolParameter[tmp.size()];
                for(int j=0;j < tmp.size();j++)
                    params[j]=tmp.get(j);
                ProtocolData data=new ProtocolData(protocol, protocol, params);
                prot_data.add(data);
            }

            ProtocolData[] data=new ProtocolData[(prot_data.size())];
            for(int k=0;k < prot_data.size();k++)
                data[k]=prot_data.get(k);
            configurator=new XmlConfigurator("bla", data);
        }
        catch(Exception x) {
            if(x instanceof java.io.IOException)
                throw (java.io.IOException)x;
            else {
                IOException tmp=new IOException();
                tmp.initCause(x);
                throw tmp;
            }
        }
        return configurator;
    }



    protected static ProtocolParameter[] parseProtocolParameters(Element protparams) throws IOException {
        try {
            Vector<ProtocolParameter> v=new Vector<ProtocolParameter>();
            protparams.normalize();
            NodeList parameters=protparams.getChildNodes();
            for(int i=0;i < parameters.getLength();i++) {
                if(parameters.item(i).getNodeType() == Node.ELEMENT_NODE) {
                    String pname=parameters.item(i).getAttributes().getNamedItem(ATTR_NAME).getNodeValue();
                    String pvalue=parameters.item(i).getAttributes().getNamedItem(ATTR_VALUE).getNodeValue();
                    ProtocolParameter p=new ProtocolParameter(pname, pvalue);
                    v.addElement(p);
                }//end if
            }//for
            ProtocolParameter[] result=new ProtocolParameter[v.size()];
            v.copyInto(result);
            return result;
        }
        catch(Exception x) {
            IOException tmp=new IOException();
            tmp.initCause(x);
            throw tmp;
        }
    }

    public static void main(String[] args) throws Exception {
        String input_file=null;
        XmlConfigurator conf;
        boolean old_format=false;

        for(int i=0;i < args.length;i++) {
            if(args[i].equals("-old")) {
                old_format=true;
                continue;
            }
            if(args[i].equals("-file")) {
                input_file=args[++i];
                continue;
            }
            help();
            return;
        }

        if(input_file != null) {
            InputStream input=null;

            try {
                input=new FileInputStream(new File(input_file));
            }
            catch(Throwable t) {
            }
            if(input == null) {
                try {
                    input=new URL(input_file).openStream();
                }
                catch(Throwable t) {
                }
            }

            if(input == null)
                input=Thread.currentThread().getContextClassLoader().getResourceAsStream(input_file);

            if(old_format) {
                Configurator config=new Configurator();
                String cfg=inputAsString(input);
                Vector<ProtocolConfiguration> tmp=config.parseConfigurations(cfg);
                System.out.println(dump(tmp));

                //                conf=XmlConfigurator.getInstanceOldFormat(input);
                //                output=conf.getProtocolStackString(true);
                //                output=replace(output, "org.jgroups.protocols.", "");
                //                System.out.println(getTitle(input_file));
                //                System.out.println('\n' + output);
            }
            else {
                conf=XmlConfigurator.getInstance(input);
                String tmp=conf.getProtocolStackString();
                System.out.println("\n" + tmp);
            }
        }
        else {
            log.error("no input file given");
        }
    }

    /**
     * @param tmp
     *                Vector of Configurator.ProtocolConfiguration
     * @return String (XML format)
     */
    private static String dump(Vector<ProtocolConfiguration> tmp) {
        StringBuilder sb=new StringBuilder();
        String indent="  ";
        sb.append("<config>\n");

        for(Iterator<ProtocolConfiguration> it=tmp.iterator();it.hasNext();) {
            Configurator.ProtocolConfiguration cfg=it.next();
            sb.append(indent).append("<").append(cfg.getProtocolName());
            Properties props=cfg.getProperties();
            if(props.isEmpty()) {
                sb.append(" />\n");
            }
            else {
                sb.append("\n").append(indent).append(indent);
                for(Iterator<Entry<Object,Object>> it2=props.entrySet().iterator();it2.hasNext();) {
                    Entry<Object,Object> entry=it2.next();
                    String key=(String)entry.getKey();
                    String val=(String)entry.getValue();
                    key=trim(key);
                    val=trim(val);
                    sb.append(key).append("=\"").append(val).append("\"");
                    if(it2.hasNext()) {
                        sb.append("\n").append(indent).append(indent);
                    }
                }
                sb.append(" />\n");
            }
        }

        sb.append("</config>\n");
        return sb.toString();
    }

    private static String trim(String val) {
        String retval="";
        int index;

        val=val.trim();
        while(true) {
            index=val.indexOf('\n');
            if(index == -1) {
                retval+=val;
                break;
            }
            retval+=val.substring(0, index);
            val=val.substring(index + 1);
        }

        return retval;
    }

    private static String inputAsString(InputStream input) throws IOException {
        int len=input.available();
        byte[] buf=new byte[len];
        input.read(buf, 0, len);
        return new String(buf);
    }

    public static String replace(String input, final String expr, String replacement) {
        StringBuilder sb=new StringBuilder();
        int new_index=0, index=0, len=expr.length(), input_len=input.length();

        while(true) {
            new_index=input.indexOf(expr, index);
            if(new_index == -1) {
                sb.append(input.substring(index, input_len));
                break;
            }
            sb.append(input.substring(index, new_index));
            sb.append(replacement);
            index=new_index + len;
        }

        return sb.toString();
    }

    static void help() {
        System.out.println("XmlConfigurator -file <input XML file> [-old]");
        System.out.println("(-old: converts old (plain-text) input format into new XML format)");
    }
}
