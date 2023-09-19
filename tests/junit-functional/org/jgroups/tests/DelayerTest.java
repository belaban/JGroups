package org.jgroups.tests;

import org.jgroups.Global;
import org.jgroups.util.Delayer;
import org.jgroups.util.Util;
import org.testng.annotations.AfterTest;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;
import java.util.function.Predicate;

/**
 * Tests {@link org.jgroups.util.Delayer}
 * @author Bela Ban
 * @since  5.3.1
 */
@Test(groups= Global.FUNCTIONAL)
public class DelayerTest {
    protected Delayer<String>         delayer;
    protected final Predicate<String> delayedTrue=k -> {Util.sleep(500); return true;};

    @BeforeTest protected void setup()   {delayer=new Delayer<>(2000);}
    @AfterTest  protected void destroy() {delayer.clear();}

    public void testAdd() throws TimeoutException {
        delayer.add("nyc", delayedTrue, success -> System.out.printf("action: success=%b\n", success));
        assert delayer.size() == 1;
        delayer.add("nyc", delayedTrue, success -> System.out.printf("action2: success=%b\n", success));
        assert delayer.size() == 1;
        delayer.add("sfo",delayedTrue, success -> System.out.printf("action3: success=%b\n", success));
        assert delayer.size() == 2;
        Util.waitUntil(delayer.timeout(), delayer.timeout() / 10, () -> delayer.size() == 0);
    }

    public void testFakeRoute() throws TimeoutException {
        final Map<String,String> routes=new ConcurrentHashMap<>();
        final AtomicBoolean result=new AtomicBoolean(false);
        final Predicate<String> pred=k -> routes.containsKey("nyc");
        final Consumer<Boolean> action=success -> {
            if(success) {
                System.out.printf("route %s found in routes: %s\n", "nyc", routes);
                result.set(true);
            }
            else
                System.out.printf("route %s not found in routes: %s\n", "nyc", routes);
        };
        delayer.add("nyc", pred, action);
        Util.sleep(3000);
        assert result.get() == false;

        delayer.add("nyc", pred, action);
        Util.sleep(500);
        System.out.println("-- adding route to NYC:");
        routes.put("nyc", "route to NYC");
        Util.waitUntil(3000, 10, result::get);
        assert delayer.size() == 0;
    }
}
