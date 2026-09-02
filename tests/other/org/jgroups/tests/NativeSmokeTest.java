package org.jgroups.tests;

import org.jgroups.Version;
import org.jgroups.conf.ClassConfigurator;

/**
 * Smoke test for native-image compilation. Verifies that essential JGroups classes load correctly
 * when compiled with GraalVM native-image, confirming that the reachability metadata is correct.
 * @author Tristan Tarrant
 */
public class NativeSmokeTest {

    public static void main(String[] args) throws Exception {
        int errors = 0;

        // Verify Version loads (requires JGROUPS_VERSION.properties as a resource)
        try {
            String version = Version.printVersion();
            if (version == null || version.isEmpty())
                throw new IllegalStateException("Version string is null or empty");
            System.out.println("OK: Version = " + version);
        } catch (Throwable t) {
            System.err.println("FAIL: Version loading failed: " + t);
            errors++;
        }

        // Verify ClassConfigurator loaded (requires jg-magic-map.xml and jg-protocol-ids.xml as resources,
        // and reflection access to all mapped classes)
        try {
            short magic = ClassConfigurator.getMagicNumber(org.jgroups.BytesMessage.class);
            if (magic <= 0)
                throw new IllegalStateException("BytesMessage magic number not found");
            Object obj = ClassConfigurator.create(magic);
            if (obj == null)
                throw new IllegalStateException("ClassConfigurator.create() returned null");
            System.out.println("OK: ClassConfigurator magic map loaded, BytesMessage magic=" + magic);
        } catch (Throwable t) {
            System.err.println("FAIL: ClassConfigurator loading failed: " + t);
            errors++;
        }

        // Verify protocol IDs loaded
        try {
            short protId = ClassConfigurator.getProtocolId(org.jgroups.protocols.UDP.class);
            if (protId <= 0)
                throw new IllegalStateException("UDP protocol ID not found");
            System.out.println("OK: Protocol IDs loaded, UDP id=" + protId);
        } catch (Throwable t) {
            System.err.println("FAIL: Protocol ID loading failed: " + t);
            errors++;
        }

        if (errors > 0) {
            System.err.println(errors + " test(s) failed");
            System.exit(1);
        }
        System.out.println("All native smoke tests passed");
    }
}
