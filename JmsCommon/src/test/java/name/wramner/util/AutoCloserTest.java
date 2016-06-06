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
package name.wramner.util;

import static org.junit.Assert.assertTrue;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.junit.Test;

/**
 * Test the {@link AutoCloser}.
 * 
 * @author Erik Wramner
 */
public class AutoCloserTest {

    @Test(expected = IllegalStateException.class)
    public void testNoCloseMethod() {
        try (AutoCloser a = new AutoCloser(new NoCloseMethodResource())) {
        }
    }

    @Test
    public void testClose() {
        List<CloseMethodResource> resources = new ArrayList<>();
        try (AutoCloser a = new AutoCloser()) {
            resources.add(new CloseMethodResource(false));
            resources.add(new CloseMethodResource(true));
            resources.add(new CloseableResource(false));
            resources.add(new CloseableResource(true));
            for (CloseMethodResource r : resources) {
                a.add(r);
            }
        }
        for (CloseMethodResource r : resources) {
            assertTrue(r.isClosed());
        }
    }

    private static class NoCloseMethodResource {
    }

    private static class CloseMethodResource {
        private final boolean _throw;
        private boolean _closed;

        public CloseMethodResource(boolean shouldThrow) {
            _throw = shouldThrow;
        }

        public void close() throws IOException {
            _closed = true;
            if (_throw) {
                throw new IOException("Failed to close!");
            }
        }

        public boolean isClosed() {
            return _closed;
        }
    }

    private static class CloseableResource extends CloseMethodResource implements Closeable {
        public CloseableResource(boolean shouldThrow) {
            super(shouldThrow);
        }
    }
}
