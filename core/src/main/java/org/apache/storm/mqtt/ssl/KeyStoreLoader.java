package org.apache.storm.mqtt.ssl;

import java.io.IOException;
import java.io.InputStream;
import java.io.Serializable;

/**
 * Abstraction for loading keystore/truststore data. This allows keystores
 * to be loaded from different sources (File system, HDFS, etc.).
 */
public interface KeyStoreLoader extends Serializable {

    String keyStorePassword();
    String trustStorePassword();
    String keyPassword();
    InputStream keyStoreInputStream() throws IOException;
    InputStream trustStoreInputStream() throws IOException;
}
