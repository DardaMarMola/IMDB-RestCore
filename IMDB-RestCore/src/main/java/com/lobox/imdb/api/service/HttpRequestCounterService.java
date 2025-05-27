package com.lobox.imdb.api.service;

import org.springframework.stereotype.Service;
import java.util.concurrent.atomic.AtomicLong;

@Service
public class HttpRequestCounterService {

    private final AtomicLong requestCount = new AtomicLong(0);

    public void increment() {
        requestCount.incrementAndGet();
    }

    public long getCount() {
        return requestCount.get();
    }

    public void reset() {
        requestCount.set(0);
    }
}