package org.apache.storm.mqtt.ssl;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.InputStream;

/**
 *
 */
public class DefaultKeyStoreLoader implements KeyStoreLoader {
    private String ksFile = null;
    private String tsFile = null;

    public DefaultKeyStoreLoader(String keystore){
        this.ksFile = keystore;
    }

    public DefaultKeyStoreLoader(String keystore, String truststore){
        this.ksFile = keystore;
        this.tsFile = truststore;
    }

    @Override
    public InputStream keyStoreInputStream() throws FileNotFoundException {
        return new FileInputStream(this.ksFile);
    }

    @Override
    public InputStream trustStoreInputStream() throws FileNotFoundException {
        // if no truststore file, assume the truststore is the keystore.
        if(this.tsFile == null){
            return new FileInputStream(this.ksFile);
        } else {
            return new FileInputStream(this.tsFile);
        }
    }
}
