/*
 * Copyright 2016 Erik Wramner.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package name.wramner.jmstools.messages;

import java.math.BigInteger;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

/**
 * Base class for message data classes. Calculates and caches an MD5 checksum for the payload as well as the payload
 * size in bytes.
 * 
 * @author Erik Wramner
 */
public class ChecksummedMessageData {
    private final String _checksum;
    private final int _length;

    /**
     * Constructor.
     * 
     * @param data The payload as raw bytes.
     */
    public ChecksummedMessageData(byte[] data) {
        _checksum = calculateChecksum(data);
        _length = data.length;
    }

    /**
     * Get checksum.
     * 
     * @return checksum.
     */
    public String getChecksum() {
        return _checksum;
    }

    /**
     * Get length of message data in bytes.
     * 
     * @return length.
     */
    public int getLength() {
        return _length;
    }

    /**
     * Compute checksum.
     * 
     * @param data The raw bytes.
     * @return checksum in text format.
     */
    public static String calculateChecksum(byte[] data) {
        try {
            return String.format("%032x", new BigInteger(1, MessageDigest.getInstance("MD5").digest(data)));
        } catch (NoSuchAlgorithmException e) {
            throw new java.lang.IllegalStateException("JVM does not support MD5", e);
        }
    }
}