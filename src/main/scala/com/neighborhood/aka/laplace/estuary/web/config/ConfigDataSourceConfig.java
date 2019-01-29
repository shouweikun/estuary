package com.neighborhood.aka.laplace.estuary.web.config;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.*;
import org.springframework.core.env.Environment;
import org.springframework.jdbc.core.JdbcTemplate;

import javax.annotation.Resource;
import javax.sql.DataSource;

/**
 * Created by meyer on 2017/1/15.
 */
@Configuration
@PropertySource("application.properties")
@EnableConfigurationProperties
public class ConfigDataSourceConfig {

    @Resource
    private Environment env;


    @Primary
    @Bean(name = "configDataSource")
    public DataSource dataSource1() {
        final String url = this.env.getProperty("spring.config.datasource.url");
        final String username = this.env.getProperty("spring.config.datasource.username");
        final String password = this.env.getProperty("spring.config.datasource.password");
        final HikariConfig config = new HikariConfig();
        config.setJdbcUrl(url);
        config.setUsername(username);
        config.setPassword(password);
        config.setMaximumPoolSize(5);
        DataSource dds = new HikariDataSource(config);
        return dds;
    }



    @Bean(name = "configJdbcTemplate")
    public JdbcTemplate JdbcTemplate1(@Qualifier("configDataSource") DataSource dataSource) {
        return new JdbcTemplate(dataSource);
    }


}