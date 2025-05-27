package com.lobox.imdb.api.interceptor;

import com.lobox.imdb.api.service.HttpRequestCounterService;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import org.springframework.stereotype.Component;
import org.springframework.web.servlet.HandlerInterceptor;

@Component
public class RequestCounterInterceptor implements HandlerInterceptor {

    private final HttpRequestCounterService counterService;

    public RequestCounterInterceptor(HttpRequestCounterService counterService) {
        this.counterService = counterService;
    }

    @Override
    public boolean preHandle(HttpServletRequest request, HttpServletResponse response, Object handler) {
        counterService.increment();
        return true;
    }
}