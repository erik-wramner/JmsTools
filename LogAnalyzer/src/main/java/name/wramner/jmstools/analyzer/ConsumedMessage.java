/*
 * Copyright 2018 Erik Wramner.
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
package name.wramner.jmstools.analyzer;

import java.sql.Timestamp;

/**
 * A consumed message.
 */
public class ConsumedMessage extends Message {
    private final Timestamp _consumedTimestamp;

    /**
     * Constructor.
     *
     * @param jmsId The unique JMS id for the message.
     * @param applicationId The application id from the JmsTools header.
     * @param payloadSize The payload size in bytes.
     * @param consumedTimestamp The time when the message was consumed.
     */
    public ConsumedMessage(String jmsId, String applicationId, Integer payloadSize, Timestamp consumedTimestamp) {
        super(jmsId, applicationId, payloadSize);
        _consumedTimestamp = consumedTimestamp;
    }

    public Timestamp getConsumedTimestamp() {
        return _consumedTimestamp;
    }
}