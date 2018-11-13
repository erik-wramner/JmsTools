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

import java.sql.SQLException;

/**
 * This exception wraps a normal SQL exception but is unchecked.
 */
public class UncheckedSqlException extends RuntimeException {
    private static final long serialVersionUID = 1L;

    public UncheckedSqlException(SQLException cause) {
        super(cause);
    }

    public UncheckedSqlException(String message, SQLException cause) {
        super(message, cause);
    }
}