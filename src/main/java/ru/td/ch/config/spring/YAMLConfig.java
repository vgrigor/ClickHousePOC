package ru.td.ch.config.spring;

import lombok.Getter;
import lombok.Setter;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Configuration;

import java.util.ArrayList;
import java.util.List;

@Configuration
@EnableConfigurationProperties
@ConfigurationProperties
@Getter
@Setter
public class YAMLConfig {

    @Value("${join.cardinality}")
    private long cardinality;
    @Value("${join.size}")
    private long  size;


    private String name;
    private String environment;
    private List<String> servers = new ArrayList<>();

    // standard getters and setters
    public void run(YAMLConfig myConfig) throws Exception {

/*        System.out.println("cardinality: " + myConfig.getCardinality());
        System.out.println("using environment: " + myConfig.getSize());

        System.out.println("using environment: " + myConfig.getEnvironment());
        System.out.println("name: " + myConfig.getName());
        System.out.println("servers: " + myConfig.getServers());*/
    }

}