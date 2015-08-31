package org.apache.storm.mqtt.ssl;

import java.io.FileNotFoundException;
import java.io.InputStream;

/**
 *
 */
public interface KeyStoreLoader {

    InputStream keyStoreInputStream() throws FileNotFoundException;
    InputStream trustStoreInputStream() throws FileNotFoundException;
}
