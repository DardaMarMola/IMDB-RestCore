package com.lobox.imdb.api.config;

import com.lobox.imdb.api.interceptor.RequestCounterInterceptor;
import org.springframework.context.annotation.Configuration;
import org.springframework.web.servlet.config.annotation.InterceptorRegistry;
import org.springframework.web.servlet.config.annotation.WebMvcConfigurer;

@Configuration
public class WebConfig implements WebMvcConfigurer {

    private final RequestCounterInterceptor requestCounterInterceptor;

    public WebConfig(RequestCounterInterceptor requestCounterInterceptor) {
        this.requestCounterInterceptor = requestCounterInterceptor;
    }

    @Override
    public void addInterceptors(InterceptorRegistry registry) {
        registry.addInterceptor(requestCounterInterceptor);
    }
}