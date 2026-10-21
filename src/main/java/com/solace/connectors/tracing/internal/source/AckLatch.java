package com.solace.connectors.tracing.internal.source;

import java.util.concurrent.CountDownLatch;

/** Hands the flow outcome from Mule's callback threads back to the Solace dispatcher thread. First release wins. */
final class AckLatch {

    private final CountDownLatch latch = new CountDownLatch(1);
    private volatile boolean success;

    synchronized void release(boolean success) {
        if (latch.getCount() > 0) {
            this.success = success;
            latch.countDown();
        }
    }

    boolean await() throws InterruptedException {
        latch.await();
        return success;
    }
}
