package org.jgroups.protocols;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;

import org.jgroups.Global;
import org.jgroups.demos.KeyStoreGenerator.KeyStoreGeneratorBuilder;
import org.testng.annotations.AfterGroups;
import org.testng.annotations.BeforeGroups;

import static org.jgroups.demos.KeyStoreGenerator.newKeyStoreGeneratorBuilder;


public class EncryptGroupUtil {

    static final String KEY_STORE_FILE=System.getProperty("keystore.dir") +File.separator + "defaultStore.keystore";
    static final String KEY_STORE2_FILE=System.getProperty("keystore.dir") +File.separator + "defaultStore2.keystore";
    static final String ENCRYTP_ALGORITHM="AES";
    static final String DEF_PWD="changeit";
    static final String KEYSTORE_TYPE="JCEKS";
    static final String KEY_ALIAS="myKey";

    @BeforeGroups(groups=Global.ENCRYPT, alwaysRun = true)
    public void setupEncrypt() throws Exception {
        KeyStoreGeneratorBuilder builder = newKeyStoreGeneratorBuilder();
        // common
        builder.symAlg(ENCRYTP_ALGORITHM)
            .storeType(KEYSTORE_TYPE)
            .storePass(DEF_PWD)
            .alias(KEY_ALIAS);

        // files generation
        builder.keyStoreName(KEY_STORE_FILE).build().generate();
        builder.keyStoreName(KEY_STORE2_FILE).build().generate();
    }

    @AfterGroups(groups=Global.ENCRYPT, alwaysRun = true)
    public void tearDownEncrypt() throws Exception {
        Files.deleteIfExists(Path.of(KEY_STORE_FILE));
        Files.deleteIfExists(Path.of(KEY_STORE2_FILE));
    }

}
