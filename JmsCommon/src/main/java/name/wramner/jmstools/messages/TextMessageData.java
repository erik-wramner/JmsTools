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

import java.nio.charset.Charset;

public class TextMessageData extends ChecksummedMessageData {
    private final String _data;

    public TextMessageData(String data) {
        super(data.getBytes(Charset.forName("UTF-8")));
        _data = data;
    }

    public String getData() {
        return _data;
    }
}