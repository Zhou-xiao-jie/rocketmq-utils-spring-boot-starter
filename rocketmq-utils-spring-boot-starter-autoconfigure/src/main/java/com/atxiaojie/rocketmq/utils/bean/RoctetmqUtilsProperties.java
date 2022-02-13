package com.atxiaojie.rocketmq.utils.bean;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

/**
 * @ClassName: RoctetmqUtilsProperties
 * @Description: RoctetmqUtils
 * @author: zhouxiaojie
 * @date: 2021/11/26 10:40
 * @Version: V1.0.0
 */
@Component
@ConfigurationProperties("atxiaojie.rocketmqutils")
public class RoctetmqUtilsProperties {

    private String consumerGroup;
    private String producerGroup;
    private String namesrvAddr;

    public String getConsumerGroup() {
        return consumerGroup;
    }

    public void setConsumerGroup(String consumerGroup) {
        this.consumerGroup = consumerGroup;
    }

    public String getProducerGroup() {
        return producerGroup;
    }

    public void setProducerGroup(String producerGroup) {
        this.producerGroup = producerGroup;
    }

    public String getNamesrvAddr() {
        return namesrvAddr;
    }

    public void setNamesrvAddr(String namesrvAddr) {
        this.namesrvAddr = namesrvAddr;
    }
}
