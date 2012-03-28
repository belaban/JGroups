package org.jgroups.util;

import org.jgroups.annotations.Experimental;
import org.jgroups.annotations.Property;
import org.jgroups.annotations.Unsupported;
import org.jgroups.logging.Log;
import org.jgroups.logging.LogFactory;
import org.jgroups.stack.Protocol;
import org.w3c.dom.DOMImplementation;
import org.w3c.dom.Document;
import org.w3c.dom.Element;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.transform.OutputKeys;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerException;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;
import java.io.*;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.*;

/**
 * Iterates over all concrete Protocol classes and creates tables with Protocol's properties.
 * These tables are in turn then merged into docbook. 
 * 
 * Iterates over unsupported and experimental classes and creates tables listing those classes.
 * These tables are in turn then merged into docbook.  
 * 
 * @author Vladimir Blagojevic
 * 
 */
public class PropertiesToXML {

    protected static final Log log = LogFactory.getLog(PropertiesToXML.class);

    public static void main(String[] args) {
        String input = "doc/manual/en/modules/protocols.xml";
        String input2 = "doc/manual/en/modules/installation.xml";     

        if (args.length != 1) {
            help();
            return;
        }
        input = args[0];
        String temp_file = input + ".tmp";       
        String temp_file2 = input2 + ".tmp";

        try {
            // first copy protocols.xml file into protocols-temp.xml
            File f = new File(temp_file);
            copy(new FileReader(new File(input)), new FileWriter(f));
            String s = fileToString(f);

            Set<Class<Protocol>> classes = Util.findClassesAssignableFrom("org.jgroups.protocols",Protocol.class);
            classes.addAll(Util.findClassesAssignableFrom("org.jgroups.protocols.pbcast",Protocol.class));
            classes.addAll(Util.findClassesAssignableFrom("org.jgroups.protocols.relay",Protocol.class));
            Properties props = new Properties();
            for (Class<Protocol> clazz : classes) {
                convertToDocbookTable(props, clazz);
            }

            String result = Util.replaceProperties(s, props);
            FileWriter fw = new FileWriter(f, false);
            fw.write(result);
            fw.flush();
            fw.close();
            
            
            
            // copy installation.xml file into installation-temp.xml
            f = new File(temp_file2);
            copy(new FileReader(new File(input2)), new FileWriter(f));
            s = fileToString(f);
            
            props = new Properties();
            List<Class<?>> unsupportedClasses = Util.findClassesAnnotatedWith("org.jgroups",Unsupported.class);
            convertToDocbookTable(props, unsupportedClasses, "Unsupported");
            List<Class<?>> experimentalClasses = Util.findClassesAnnotatedWith("org.jgroups",Experimental.class);
            convertToDocbookTable(props, experimentalClasses, "Experimental");
            
            result = Util.replaceProperties(s, props);
            fw = new FileWriter(f, false);
            fw.write(result);
            fw.flush();
            fw.close();
            
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    static void help() {
        System.out.println("PropertiesToXML <input XML file>");
    }

    
    

    
    private static void convertToDocbookTable(Properties props, List<Class<?>> clazzes, String title)
                    throws ParserConfigurationException, TransformerException {

        Document xmldoc = null;
        DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
        DocumentBuilder builder = factory.newDocumentBuilder();
        DOMImplementation impl = builder.getDOMImplementation();
        xmldoc = impl.createDocument(null, "table", null);
        Element tbody = createXMLTable(xmldoc, title);

        for (Class<?> clazz : clazzes) {
            Element row = xmldoc.createElement("row");
            Element entry = xmldoc.createElement("entry");
            entry.setTextContent(clazz.getPackage().getName());
            row.appendChild(entry);

            entry = xmldoc.createElement("entry");
            entry.setTextContent(clazz.getSimpleName());
            row.appendChild(entry);
            tbody.appendChild(row);
        }

        // do we have more than one property (superclass Protocol has only one property (stats))
        if (clazzes.size() > 1) {
            DOMSource domSource = new DOMSource(xmldoc);
            StringWriter sw = new StringWriter();
            StreamResult streamResult = new StreamResult(sw);
            TransformerFactory tf = TransformerFactory.newInstance();
            Transformer serializer = tf.newTransformer();
            serializer.setOutputProperty(OutputKeys.ENCODING, "ISO-8859-1");
            serializer.setOutputProperty(OutputKeys.INDENT, "yes");
            serializer.transform(domSource, streamResult);
            StringBuffer buffer = sw.getBuffer();
            buffer.delete(0, buffer.indexOf("table") - 1);
            props.put(title, buffer.toString());
        }
    }

    private static void convertToDocbookTable(Properties props, Class<Protocol> clazz) throws Exception {
        boolean isExperimental = clazz.isAnnotationPresent(Experimental.class);
        boolean isUnsupported = clazz.isAnnotationPresent(Unsupported.class);
        if (isUnsupported)
            return;
        Document xmldoc = null;
        DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
        DocumentBuilder builder = factory.newDocumentBuilder();
        DOMImplementation impl = builder.getDOMImplementation();
        xmldoc = impl.createDocument(null, "table", null);
        Element tbody = createXMLTree(xmldoc, isExperimental);
        Map<String, String> nameToDescription = new TreeMap<String, String>();

        Field[] fields = clazz.getDeclaredFields();
        for (Field field : fields) {
            if (field.isAnnotationPresent(Property.class)) {
                String property = field.getName();
                Property annotation = field.getAnnotation(Property.class);
                String desc = annotation.description();
                nameToDescription.put(property, desc);
            }
        }

        // iterate methods
        Method[] methods = clazz.getDeclaredMethods();
        for (Method method : methods) {
            if (method.isAnnotationPresent(Property.class)
              && method.getName().startsWith("set")) {

                Property annotation = method.getAnnotation(Property.class);
                String desc = annotation.description();

                if(desc == null || desc.length() == 0)
                    desc="n/a";

                String name = annotation.name();
                if (name.length() < 1) {
                    name = Util.methodNameToAttributeName(method.getName());
                }
                nameToDescription.put(name, desc);
            }
        }

        // and write them out
        for (Map.Entry<String, String> e : nameToDescription.entrySet()) {
            Element row = xmldoc.createElement("row");
            Element entry = xmldoc.createElement("entry");
            entry.setTextContent(e.getKey());
            row.appendChild(entry);

            entry = xmldoc.createElement("entry");
            entry.setTextContent(e.getValue());
            row.appendChild(entry);
            tbody.appendChild(row);
        }

        // do we have more than one property (superclass Protocol has only one property (stats))
        if (!nameToDescription.isEmpty()) {
            DOMSource domSource = new DOMSource(xmldoc);
            StringWriter sw = new StringWriter();
            StreamResult streamResult = new StreamResult(sw);
            TransformerFactory tf = TransformerFactory.newInstance();
            Transformer serializer = tf.newTransformer();
            serializer.setOutputProperty(OutputKeys.ENCODING, "ISO-8859-1");
            serializer.setOutputProperty(OutputKeys.INDENT, "yes");
            serializer.transform(domSource, streamResult);
            StringBuffer buffer = sw.getBuffer();
            buffer.delete(0, buffer.indexOf("table") - 1);
            props.put(clazz.getSimpleName(), buffer.toString());
        }
    }

    private static String fileToString(File f) throws Exception {
        StringWriter output = new StringWriter();
        FileReader input = new FileReader(f);
        char[] buffer = new char[8 * 1024];
        int n = 0;
        while (-1 != (n = input.read(buffer))) {
            output.write(buffer, 0, n);
        }
        return output.toString();
    }

    public static int copy(Reader input, Writer output) throws IOException {
        char[] buffer = new char[8 * 1024];
        int count = 0;
        int n = 0;
        try {
            while (-1 != (n = input.read(buffer))) {
                output.write(buffer, 0, n);
                count += n;
            }
        } finally {
            output.flush();
            output.close();
        }
        return count;
    }

    private static Element createXMLTree(Document xmldoc, boolean experimental)
                    throws ParserConfigurationException {

        Element root = xmldoc.getDocumentElement();
        Element title = xmldoc.createElement("title");
        if (experimental)
            title.setTextContent("Properties (experimental)");
        else
            title.setTextContent("Properties");
        root.appendChild(title);

        Element tgroup = xmldoc.createElement("tgroup");
        tgroup.setAttribute("cols", "2");
        root.appendChild(tgroup);

        Element colspec = xmldoc.createElement("colspec");
        colspec.setAttribute("align", "left");
        tgroup.appendChild(colspec);

        Element thead = xmldoc.createElement("thead");
        tgroup.appendChild(thead);

        Element row = xmldoc.createElement("row");
        thead.appendChild(row);

        Element entry = xmldoc.createElement("entry");
        entry.setAttribute("align", "center");
        entry.setTextContent("Name");
        row.appendChild(entry);

        entry = xmldoc.createElement("entry");
        entry.setAttribute("align", "center");
        entry.setTextContent("Description");
        row.appendChild(entry);

        Element tbody = xmldoc.createElement("tbody");
        tgroup.appendChild(tbody);

        return tbody;
    }

    private static Element createXMLTable(Document xmldoc, String titleContent)
                    throws ParserConfigurationException {

        Element root = xmldoc.getDocumentElement();
        Element title = xmldoc.createElement("title");
        title.setTextContent(titleContent);
        root.appendChild(title);

        Element tgroup = xmldoc.createElement("tgroup");
        tgroup.setAttribute("cols", "2");
        root.appendChild(tgroup);

        Element colspec = xmldoc.createElement("colspec");
        colspec.setAttribute("align", "left");
        tgroup.appendChild(colspec);

        Element thead = xmldoc.createElement("thead");
        tgroup.appendChild(thead);

        Element row = xmldoc.createElement("row");
        thead.appendChild(row);

        Element entry = xmldoc.createElement("entry");
        entry.setAttribute("align", "center");
        entry.setTextContent("Package");
        row.appendChild(entry);

        entry = xmldoc.createElement("entry");
        entry.setAttribute("align", "center");
        entry.setTextContent("Class");
        row.appendChild(entry);

        Element tbody = xmldoc.createElement("tbody");
        tgroup.appendChild(tbody);

        return tbody;
    }
}
