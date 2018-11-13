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
 * Produced message.
 */
public class ProducedMessage extends Message {
    private final int _delaySeconds;
    private final Timestamp _publishedTimestamp;

    public ProducedMessage(String jmsId, String applicationId, Integer payloadSize, Timestamp publishedTimestamp,
                    int delaySeconds) {
        super(jmsId, applicationId, payloadSize);
        _delaySeconds = delaySeconds;
        _publishedTimestamp = publishedTimestamp;
    }

    public int getDelaySeconds() {
        return _delaySeconds;
    }

    public Timestamp getPublishedTimestamp() {
        return _publishedTimestamp;
    }
}