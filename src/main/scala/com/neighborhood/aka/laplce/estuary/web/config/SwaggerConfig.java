package com.neighborhood.aka.laplce.estuary.web.config;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import springfox.documentation.builders.ApiInfoBuilder;
import springfox.documentation.builders.PathSelectors;
import springfox.documentation.service.ApiInfo;
import springfox.documentation.spi.DocumentationType;
import springfox.documentation.spring.web.plugins.Docket;
import springfox.documentation.swagger2.annotations.EnableSwagger2;

/**
 * Created by john_liu on 2017/11/6.
 */

@Configuration
@EnableSwagger2
public class SwaggerConfig {

    @Bean
    public Docket userApi() {
        return new Docket(DocumentationType.SWAGGER_2)
                .apiInfo(apiInfo())
                .select()
                .paths(PathSelectors.regex("/api/.*"))
                .build();
    }

    private ApiInfo apiInfo() {
        return new ApiInfoBuilder()
                .title("Estuary")
                .description("数海数据流式同步系统")
                .license("Apache License Version 2.0")
                .version("2.0")
                .build();
    }
}
