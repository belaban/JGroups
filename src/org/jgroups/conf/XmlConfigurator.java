
package org.jgroups.conf;


import org.jgroups.logging.Log;
import org.jgroups.logging.LogFactory;
import org.jgroups.util.Util;
import org.w3c.dom.*;
import org.xml.sax.ErrorHandler;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;
import org.xml.sax.SAXParseException;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.util.*;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Uses XML to configure a protocol stack
 * @author Vladimir Blagojevic
 */
public class XmlConfigurator implements ProtocolStackConfigurator {
    private static final String               JAXP_SCHEMA_LANGUAGE = "http://java.sun.com/xml/jaxp/properties/schemaLanguage";
    private static final String               W3C_XML_SCHEMA = "http://www.w3.org/2001/XMLSchema";
    private final List<ProtocolConfiguration> configuration=new ArrayList<>();
    protected static final Log                log=LogFactory.getLog(XmlConfigurator.class);

    
    protected XmlConfigurator(List<ProtocolConfiguration> protocols) {
        configuration.addAll(protocols);
    }

    public static XmlConfigurator getInstance(InputStream stream) throws java.io.IOException {
        return getInstance(stream, false);
    }

    public static XmlConfigurator getInstance(InputStream stream, boolean validate) throws java.io.IOException {
        return parse(stream, validate);
    }

    /**
     * 
     * @param convert If false: print old plain output, else print new XML format
     * @return String with protocol stack in specified format
     */
    public String getProtocolStackString(boolean convert) {
        StringBuilder buf=new StringBuilder();
        Iterator<ProtocolConfiguration> it=configuration.iterator();
        if(convert)
            buf.append("<config>\n");
        while(it.hasNext()) {
            ProtocolConfiguration d=it.next();
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

    public List<ProtocolConfiguration> getProtocolStack() {
        return configuration;
    }



    protected static XmlConfigurator parse(InputStream stream, boolean validate) throws java.io.IOException {

        // CAUTION: crappy code ahead ! I (bela) am not an XML expert, so the code below is pretty amateurish...
        // But it seems to work, and it is executed only on startup, so no perf loss on the critical path.
        // If somebody wants to improve this, please be my guest.
        try {
            DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
            factory.setValidating(validate);
            factory.setNamespaceAware(validate);
            if(validate)
                factory.setAttribute(JAXP_SCHEMA_LANGUAGE, W3C_XML_SCHEMA);

            DocumentBuilder builder = factory.newDocumentBuilder();
            builder.setEntityResolver((publicId, systemId) -> {
                if (systemId != null && systemId.startsWith("http://www.jgroups.org/schema/JGroups-")) {
                    String schemaName = systemId.substring("http://www.jgroups.org/".length());
                    InputStream schemaIs = getAsInputStreamFromClassLoader(schemaName);
                    if (schemaIs == null) {
                        throw new IOException("Schema not found from classloader: " + schemaName);
                    }
                    InputSource source = new InputSource(schemaIs);
                    source.setPublicId(publicId);
                    source.setSystemId(systemId);
                    return source;
                }
                return null;
            });
            // Use AtomicReference to allow make variable final, not for atomicity
            // We store only last exception
            final AtomicReference<SAXParseException> exceptionRef = new AtomicReference<>();
            builder.setErrorHandler(new ErrorHandler() {

                public void warning(SAXParseException exception) throws SAXException {
                    log.warn(Util.getMessage("ParseFailure"), exception);
                }

                public void fatalError(SAXParseException exception) throws SAXException {
                    exceptionRef.set(exception);
                }

                public void error(SAXParseException exception) throws SAXException {
                    exceptionRef.set(exception);
                }
            });
            Document document = builder.parse(stream);
            if (exceptionRef.get() != null) {
                throw exceptionRef.get();
            }

            // The root element of the document should be the "config" element,
            // but the parser(Element) method checks this so a check is not
            // needed here.
            Element configElement = document.getDocumentElement();
            return parse(configElement);
        } catch (Exception x) {
            throw new IOException(String.format(Util.getMessage("ParseError"), x.getLocalizedMessage()));
        }
    }

    private static InputStream getAsInputStreamFromClassLoader(String filename) {
        ClassLoader cl = Thread.currentThread().getContextClassLoader();
        InputStream is = cl == null ? null : cl.getResourceAsStream(filename);
        if (is == null) {
            // check system class loader
            is = XmlConfigurator.class.getClassLoader().getResourceAsStream(filename);
        }
        return is;
    }
    
    protected static XmlConfigurator parse(Element root_element) throws Exception {
        return new XmlConfigurator(parseProtocols(root_element));
    }


    public static List<ProtocolConfiguration> parseProtocols(Element root_element) throws Exception {
        // CAUTION: crappy code ahead ! I (bela) am not an XML expert, so the code below is pretty amateurish...
        // But it seems to work, and it is executed only on startup, so no perf loss on the critical path.
        // If somebody wants to improve this, please be my guest.
        String root_name=root_element.getNodeName().trim().toLowerCase();
        if(!"config".equals(root_name))
            throw new IOException("the configuration does not start with a <config> element: " + root_name);

        final List<ProtocolConfiguration> prot_data=new ArrayList<>();
        NodeList prots=root_element.getChildNodes();
        for(int i=0;i < prots.getLength();i++) {
            Node node=prots.item(i);
            if(node.getNodeType() != Node.ELEMENT_NODE)
                continue;

            Element tag=(Element)node;
            String  protocol=tag.getTagName();
            Map<String,String> params=new HashMap<>();

            NamedNodeMap attrs=tag.getAttributes();
            int attrLength=attrs.getLength();
            for(int a=0;a < attrLength;a++) {
                Attr attr=(Attr)attrs.item(a);
                String name=attr.getName();
                String value=attr.getValue();
                params.put(name, value);
            }
            ProtocolConfiguration cfg=new ProtocolConfiguration(protocol, params);
            prot_data.add(cfg);

            // store protocol-specific configuration (if available); this will be passed to the protocol on
            // creation to to parse
            NodeList subnodes=node.getChildNodes();
            for(int j=0; j < subnodes.getLength(); j++) {
                Node subnode=subnodes.item(j);
                if(subnode.getNodeType() != Node.ELEMENT_NODE)
                    continue;
                cfg.addSubtree(subnode);
            }
        }
        return prot_data;
    }


    public static void main(String[] args) throws Exception {
        String input_file=null;
        XmlConfigurator conf;

        for(int i=0;i < args.length;i++) {
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

            conf=XmlConfigurator.getInstance(input);
            String tmp=conf.getProtocolStackString();
            System.out.println("\n" + tmp);
        }
        else
            throw new Exception("no input file given");
    }

    
    private static String dump(Collection<ProtocolConfiguration> configs) {
        StringBuilder sb=new StringBuilder();
        String indent="  ";
        sb.append("<config>\n");

        for(ProtocolConfiguration cfg: configs) {
            sb.append(indent).append("<").append(cfg.getProtocolName());
            Map<String,String> props=cfg.getProperties();
            if(props.isEmpty()) {
                sb.append(" />\n");
            }
            else {
                sb.append("\n").append(indent).append(indent);
                for(Map.Entry<String,String> entry: props.entrySet()) {
                    String key=entry.getKey();
                    String val=entry.getValue();
                    key=trim(key);
                    val=trim(val);
                    sb.append(key).append("=\"").append(val).append("\" ");
                }
                sb.append(" />\n");
            }
        }

        sb.append("</config>\n");
        return sb.toString();
    }

    private static String trim(String val) {
        StringBuilder retval=new StringBuilder();
        int index;

        val=val.trim();
        while(true) {
            index=val.indexOf('\n');
            if(index == -1) {
                retval.append(val);
                break;
            }
            retval.append(val, 0, index);
            val=val.substring(index + 1);
        }

        return retval.toString();
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
                sb.append(input, index, input_len);
                break;
            }
            sb.append(input, index, new_index);
            sb.append(replacement);
            index=new_index + len;
        }

        return sb.toString();
    }

    static void help() {
        System.out.println("XmlConfigurator -file <input XML file>");
    }
}
