package com.lobox.imdb.api.config;

import io.swagger.v3.oas.models.OpenAPI;
import io.swagger.v3.oas.models.info.Info;
import io.swagger.v3.oas.models.info.License;
import org.springdoc.core.models.GroupedOpenApi;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class OpenAPIConfig {

    @Bean
    public OpenAPI cafeOpenAPI() {
        return new OpenAPI().info(new Info().title("Internet Movie Database API").
                description("Spring IMDB API application").version("v0.0.1").
                license(new License().name("Apache 2.0").url("http://springdoc.org")));
    }

    @Bean
    public GroupedOpenApi publicApi() {
        return GroupedOpenApi.builder().group("public").pathsToMatch("/**").build();
    }
}
