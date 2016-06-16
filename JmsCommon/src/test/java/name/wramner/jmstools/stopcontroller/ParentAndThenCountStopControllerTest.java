package name.wramner.jmstools.stopcontroller;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import name.wramner.jmstools.counter.AtomicCounter;
import name.wramner.jmstools.counter.Counter;

import org.junit.Test;

/**
 * Unit test for {@link ParentAndThenCountStopController}.
 */
public class ParentAndThenCountStopControllerTest {

    @Test
    public void testShouldKeepRunning() {
        Counter c1 = new AtomicCounter();
        Counter c2 = new AtomicCounter();
        StopController parentController = new CountStopController(5, c1);
        ParentAndThenCountStopController controller = new ParentAndThenCountStopController(parentController, c2, 2);
        for (int i = 0; i < 4; i++) {
            c1.incrementCount(1);
            c2.incrementCount(1);
            assertTrue(parentController.keepRunning());
            assertTrue(controller.keepRunning());
        }
        c1.incrementCount(1);
        assertFalse(parentController.keepRunning());
        assertTrue(controller.keepRunning());
        c2.incrementCount(1);
        assertTrue(controller.keepRunning());
        c2.incrementCount(1);
        assertFalse(controller.keepRunning());
    }
}
