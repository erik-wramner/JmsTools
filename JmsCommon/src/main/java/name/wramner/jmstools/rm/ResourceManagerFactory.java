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
package name.wramner.jmstools.rm;

/**
 * A resource manager factory produces resource managers. Clear as fog. Basically it creates something that ties
 * together the JMS resources (connection, session, consumer/producer) and the active transaction and knows how to
 * release them when done.
 * 
 * @author Erik Wramner
 */
public interface ResourceManagerFactory {

    /**
     * Create resource manager.
     * 
     * @return resource manager.
     */
    ResourceManager createResourceManager();
}
