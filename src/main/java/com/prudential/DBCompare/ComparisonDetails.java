package com.prudential.DBCompare;

import com.prudential.DBCompare.config.YamlPropertySourceFactory;
import lombok.Getter;
import lombok.Setter;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.PropertySource;

import java.util.List;
import java.util.Map;

@Configuration
@ConfigurationProperties(prefix = "yaml")
@PropertySource(value = "classpath:comparisonDetails.yml", factory = YamlPropertySourceFactory.class)
public class ComparisonDetails {

    @Getter @Setter
    private String criterion;

    @Getter @Setter
    private List<Map<String,String>> tables;

}