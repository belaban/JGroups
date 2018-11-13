package org.jgroups.util;

import java.util.Objects;

/**
 * A promise which only sets the result if it matches the expected result
 * @author Bela Ban
 * @since  4.0.16
 */
public class MatchingPromise<T> extends Promise<T> {
    protected T expected_result;

    public MatchingPromise(T expected_result) {
        this.expected_result=expected_result;
    }

    public T getExpectedResult() {
        return expected_result;
    }

    /** Sets the result only if expected_result matches result */
    public void setResult(T result) {
        lock.lock();
        try {
            if(Objects.equals(expected_result, result))
                super.setResult(result);
        }
        finally {
            lock.unlock();
        }
    }

    public void reset(T expected_result) {
        lock.lock();
        try {
            this.expected_result=expected_result;
            super.reset(true);
        }
        finally {
            lock.unlock();
        }
    }

    public void reset(T expected_result, boolean signal) {
        lock.lock();
        try {
            this.expected_result=expected_result;
            super.reset(signal);
        }
        finally {
            lock.unlock();
        }
    }

    public String toString() {
        return super.toString() + String.format(" (expected result: %s)", expected_result);
    }
}
