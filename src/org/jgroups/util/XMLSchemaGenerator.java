package org.jgroups.util;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.net.URL;
import java.util.HashSet;
import java.util.Set;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.transform.OutputKeys;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;

import org.jgroups.Version;
import org.jgroups.annotations.Experimental;
import org.jgroups.annotations.Property;
import org.jgroups.annotations.Unsupported;
import org.jgroups.stack.Configurator;
import org.jgroups.stack.Protocol;
import org.w3c.dom.DOMImplementation;
import org.w3c.dom.Document;
import org.w3c.dom.Element;

/**
 * Iterates over all concrete Protocol classes and creates XML schema used for validation of 
 * configuration files. 
 * 
 * 
 * @author Vladimir Blagojevic
 * @version $Id: XMLSchemaGenerator.java,v 1.4 2009/01/21 20:52:15 vlada Exp $
 * 
 */
public class XMLSchemaGenerator {

	public static void main(String[] args) {
		
		String outputDir = "./";
		
		for(int i=0; i < args.length; i++) {
            String arg=args[i];
            if("-o".equals(arg)) {
            	outputDir=args[++i];
            	continue;
            }
		}
		
		File f = new File(outputDir,"JGroups-" + Version.major + "." + Version.minor + ".xsd");
		try {
			FileWriter fw = new FileWriter(f,false);
			DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
			DocumentBuilder builder = factory.newDocumentBuilder();
			DOMImplementation impl = builder.getDOMImplementation();
			Document xmldoc = impl.createDocument("http://www.w3.org/2001/XMLSchema", "xs:schema", null);			
			xmldoc.getDocumentElement().setAttribute("targetNamespace", "urn:org:jgroups");
		
			Element xsElement = xmldoc.createElement("xs:element");
			xsElement.setAttribute("name", "config");
			xmldoc.getDocumentElement().appendChild(xsElement);
			
			Element complexType = xmldoc.createElement("xs:complexType");
			xsElement.appendChild(complexType);
			
			Element allType = xmldoc.createElement("xs:choice");
			allType.setAttribute("maxOccurs", "unbounded");
			complexType.appendChild(allType);
			

			Set<Class<?>> classes = getClasses("org.jgroups.protocols",Protocol.class);
			for (Class<?> clazz : classes) {
				classToXML(xmldoc, allType, clazz,"");
			}
			classes = getClasses("org.jgroups.protocols.pbcast",Protocol.class);
			for (Class<?> clazz : classes) {
				classToXML(xmldoc, allType, clazz,"pbcast.");
			}
			
			DOMSource domSource = new DOMSource(xmldoc);
			StreamResult streamResult = new StreamResult(fw);
			TransformerFactory tf = TransformerFactory.newInstance();
			Transformer serializer = tf.newTransformer();
			serializer.setOutputProperty(OutputKeys.METHOD, "xml");
			serializer.setOutputProperty(OutputKeys.INDENT, "yes");
			serializer.setOutputProperty("{http://xml.apache.org/xslt}indent-amount", "1");
			serializer.transform(domSource, streamResult);
			
			fw.flush();
			fw.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	private static Set<Class<?>> getClasses(String packageName, Class<?> assignableFrom) throws IOException, ClassNotFoundException {
		ClassLoader loader = Thread.currentThread().getContextClassLoader();
		Set<Class<?>> classes = new HashSet<Class<?>>();
		String path = packageName.replace('.', '/');
		URL resource = loader.getResource(path);
		if (resource != null) {
			String filePath = resource.getFile();
			if (filePath != null && new File(filePath).isDirectory()) {
				for (String file : new File(filePath).list()) {
					if (file.endsWith(".class")) {
						String name = packageName + '.' + file.substring(0, file.indexOf(".class"));
						Class<?> clazz = Class.forName(name);
						if (assignableFrom.isAssignableFrom(clazz))
							classes.add(clazz);
					}
				}
			}
		}
		return classes;
	}

	private static void classToXML(Document xmldoc, Element parent, Class<?> clazz, String preAppendToSimpleClassName)throws Exception {

		boolean isConcreteClass = (clazz.getModifiers() & Modifier.ABSTRACT) == 0;
		boolean isExperimental = clazz.isAnnotationPresent(Experimental.class);
		boolean isUnsupported = clazz.isAnnotationPresent(Unsupported.class);
		
		if (isConcreteClass && !isExperimental && !isUnsupported) {
			parent.appendChild(createXMLTree(xmldoc, clazz, preAppendToSimpleClassName));
		} 
	}

	private static Element createXMLTree(Document xmldoc, Class<?> clazz, String preAppendToSimpleClassName)
			throws Exception {

		Element classElement = xmldoc.createElement("xs:element");
		classElement.setAttribute("name", preAppendToSimpleClassName + clazz.getSimpleName());
		
		Element complexType = xmldoc.createElement("xs:complexType");
		classElement.appendChild(complexType);
		
		
		
		// iterate fields
		for (Class<?> clazzInLoop = clazz; clazzInLoop != null; clazzInLoop = clazzInLoop.getSuperclass()) {
			Field[] fields = clazzInLoop.getDeclaredFields();
			for (Field field : fields) {
				if (field.isAnnotationPresent(Property.class)) {
					String property = field.getName();
					Property annotation = field.getAnnotation(Property.class);
					boolean annotationRedefinesName = annotation.name().length() > 0;
					if(annotationRedefinesName){
						property = annotation.name();
					}
					Element attributeElement = xmldoc.createElement("xs:attribute");
					attributeElement.setAttribute("name", property);
					
					//Agreement with Bela Ban on Jan-20-2009 (Go Obama!!!) to treat all types as
					//xs:string since we do not know where users are going to use 
					//replacement tokens in configuration files. Therefore, the type becomes indeterminate.
					//attributeElement.setAttribute("type", fieldToXMLSchemaAttributeType(field));
					attributeElement.setAttribute("type", "xs:string");
					complexType.appendChild(attributeElement);
					
					Element annotationElement = xmldoc.createElement("xs:annotation");
					attributeElement.appendChild(annotationElement);
					
					Element documentationElement = xmldoc.createElement("xs:documentation");
					documentationElement.setTextContent(annotation.description());
					annotationElement.appendChild(documentationElement);
				}
			}
		}

		// iterate methods
		Method[] methods = clazz.getMethods();
		for (Method method : methods) {
			if (method.isAnnotationPresent(Property.class) && method.getName().startsWith("set")) {

				Property annotation = method.getAnnotation(Property.class);
				String name = annotation.name();
				if (name.length() < 1) {
					name = Configurator.renameFromJavaCodingConvention(method.getName().substring(3));
				}
				Element attributeElement = xmldoc.createElement("xs:attribute");
				attributeElement.setAttribute("name", name);
				attributeElement.setAttribute("type", "xs:string");
				complexType.appendChild(attributeElement);
				
				String desc = annotation.description();
				if (desc.length() > 0) {
					Element annotationElement = xmldoc.createElement("xs:annotation");
					attributeElement.appendChild(annotationElement);
					
					Element documentationElement = xmldoc.createElement("xs:documentation");
					documentationElement.setTextContent(annotation.description());
					annotationElement.appendChild(documentationElement);
				}
			}
		}
		
		return classElement;
	}
	
	private static String fieldToXMLSchemaAttributeType(Field f){
		Class<?> propertyFieldType = f.getType();
		if(Boolean.TYPE.equals(propertyFieldType)) {
            return "xs:boolean";
        }
        else if(Integer.TYPE.equals(propertyFieldType)) {
            return "xs:integer";
        }
        else if(Long.TYPE.equals(propertyFieldType)) {
            return "xs:long";
        }
        else if(Byte.TYPE.equals(propertyFieldType)) {
            return "xs:byte";
        }
        else if(Double.TYPE.equals(propertyFieldType)) {
            return "xs:double";
        }
        else if(Short.TYPE.equals(propertyFieldType)) {
            return "xs:short";
        }
        else if(Float.TYPE.equals(propertyFieldType)) {
            return "xs:float";
        }
		return "xs:string";
	}
}
