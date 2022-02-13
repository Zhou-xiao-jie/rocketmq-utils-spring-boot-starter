package com.atxiaojie.rocketmq.utils.autoconfiguration;

import com.atxiaojie.rocketmq.utils.bean.RoctetmqUtilsProperties;
import com.atxiaojie.rocketmq.utils.service.RocketConsumer;
import com.atxiaojie.rocketmq.utils.service.RocketProducer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * @ClassName: RoctetmqUtilsAutoConfiguration
 * @Description: RoctetmqUtilsAutoConfiguration
 * @author: zhouxiaojie
 * @date: 2021/11/26 14:19
 * @Version: V1.0.0
 */
@Configuration
@EnableConfigurationProperties(RoctetmqUtilsProperties.class)
public class RoctetmqUtilsAutoConfiguration {

    @Autowired
    private RoctetmqUtilsProperties roctetmqUtilsProperties;

    @ConditionalOnMissingBean(RocketProducer.class)
    @Bean
    public RocketProducer rocketProducer(){
        return new RocketProducer(roctetmqUtilsProperties);
    }

    @ConditionalOnMissingBean(RocketConsumer.class)
    @Bean
    public RocketConsumer rocketConsumer(){
        return new RocketConsumer(roctetmqUtilsProperties, "*");
    }
}
