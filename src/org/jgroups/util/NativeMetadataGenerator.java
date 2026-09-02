package org.jgroups.util;

import org.jgroups.Version;
import org.jgroups.conf.ClassConfigurator;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;

/**
 * Generates GraalVM native-image reachability metadata (reachability-metadata.json) from jg-magic-map.xml and
 * jg-protocol-ids.xml. The generated file is placed under META-INF/native-image/org.jgroups/jgroups/ and is
 * auto-discovered by native-image when JGroups is on the classpath.
 * @author Tristan Tarrant
 */
public class NativeMetadataGenerator {

    private static final String OUTPUT_DIR = "META-INF/native-image/org.jgroups/jgroups";

    public static void main(String[] args) throws Exception {
        String outputBase = "./";
        for (int i = 0; i < args.length; i++) {
            if ("-o".equals(args[i])) {
                outputBase = args[++i];
            }
        }

        Path outputDir = Path.of(outputBase, OUTPUT_DIR);
        Files.createDirectories(outputDir);

        Set<String> classes = new TreeSet<>();
        collectClasses(ClassConfigurator.MAGIC_NUMBER_FILE, classes);
        collectClasses(ClassConfigurator.PROTOCOL_ID_FILE, classes);

        writeReachabilityMetadata(outputDir, classes);
        writeNativeImageProperties(outputDir);
    }

    private static void collectClasses(String resourceFile, Set<String> classes) throws Exception {
        try (InputStream stream = Util.getResourceAsStream(resourceFile, NativeMetadataGenerator.class)) {
            if (stream == null)
                throw new FileNotFoundException("Resource not found: " + resourceFile);
            List<Triple<Short, String, Boolean>> mappings = parseMappings(stream);
            for (Triple<Short, String, Boolean> mapping : mappings) {
                if (!mapping.val3()) // skip external classes
                    classes.add(mapping.val2());
            }
        }
    }

    private static List<Triple<Short, String, Boolean>> parseMappings(InputStream in) throws Exception {
        List<String> lines = parseLines(in);
        List<Triple<Short, String, Boolean>> retval = new ArrayList<>();
        for (String line : lines) {
            short id;
            String name;
            boolean external = false;
            int index = line.indexOf("id");
            if (index == -1) continue;
            index += 3;
            id = Short.parseShort(parseQuoted(line, index));
            index = line.indexOf("name");
            if (index == -1) continue;
            index += 5;
            name = parseQuoted(line, index);
            index = line.indexOf("external");
            if (index >= 0) {
                index += 9;
                external = Boolean.parseBoolean(parseQuoted(line, index));
            }
            retval.add(new Triple<>(id, name, external));
        }
        return retval;
    }

    private static String parseQuoted(String line, int fromIndex) {
        int start = line.indexOf('"', fromIndex);
        int end = line.indexOf('"', start + 1);
        return line.substring(start + 1, end);
    }

    private static List<String> parseLines(InputStream in) throws IOException {
        List<String> lines = new ArrayList<>();
        String content = new String(in.readAllBytes(), StandardCharsets.UTF_8);
        int pos = 0;
        while ((pos = content.indexOf("<class", pos)) >= 0) {
            int end = content.indexOf("/>", pos);
            if (end == -1) break;
            lines.add(content.substring(pos, end + 2));
            pos = end + 2;
        }
        return lines;
    }

    private static void writeReachabilityMetadata(Path outputDir, Set<String> classes) throws IOException {
        Path file = outputDir.resolve("reachability-metadata.json");
        try (PrintWriter w = new PrintWriter(Files.newBufferedWriter(file, StandardCharsets.UTF_8))) {
            w.println("{");

            // Reflection section
            w.println("  \"reflection\": [");
            Iterator<String> it = classes.iterator();
            while (it.hasNext()) {
                String cls = it.next();
                w.print("    { \"type\": \"" + cls + "\", \"methods\": [{ \"name\": \"<init>\", \"parameterTypes\": [] }] }");
                if (it.hasNext()) w.print(",");
                w.println();
            }
            w.println("  ],");

            // Resources section
            w.println("  \"resources\": [");
            String[] resources = {
                ClassConfigurator.MAGIC_NUMBER_FILE,
                ClassConfigurator.PROTOCOL_ID_FILE,
                Version.VERSION_FILE,
                "jg-messages.properties",
                "jg-messages_en.properties",
                "jg-messages_en_US.properties"
            };
            for (int i = 0; i < resources.length; i++) {
                w.print("    { \"glob\": \"" + resources[i] + "\" }");
                if (i < resources.length - 1) w.print(",");
                w.println();
            }
            w.println("  ],");

            // Bundles section
            w.println("  \"bundles\": [");
            w.println("    { \"name\": \"jg-messages\" }");
            w.println("  ]");

            w.println("}");
        }
    }

    private static void writeNativeImageProperties(Path outputDir) throws IOException {
        Path file = outputDir.resolve("native-image.properties");
        try (PrintWriter w = new PrintWriter(Files.newBufferedWriter(file, StandardCharsets.UTF_8))) {
            w.println("Args = --initialize-at-run-time=org.jgroups.util.Util");
        }
    }
}
