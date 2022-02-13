package com.atxiaojie.rocketmq.utils.service;

import com.atxiaojie.rocketmq.utils.bean.RoctetmqUtilsProperties;
import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.*;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.common.consumer.ConsumeFromWhere;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.protocol.heartbeat.MessageModel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;

/**
 * @ClassName: RocketConsumer
 * @Description: 消费者
 * @author: zhouxiaojie
 * @date: 2021/11/26 17:32
 * @Version: V1.0.0
 */
public class RocketConsumer {

    private static Logger logger = LoggerFactory.getLogger(RocketProducer.class);

    private static final String DEFAULT_TOPIC = "defaultTopic";
    private static final boolean DEFAULT_ISBROADCASTING = false;

    private DefaultMQPushConsumer consumer;
    private String topic;
    private String tags;
    //是否为广播消费模式
    private Boolean isBroadcasting;

    public String getTopic() {
        return topic;
    }

    public void setTopic(String topic) {
        this.topic = topic;
    }

    public String getTags() {
        return tags;
    }

    public void setTags(String tags) {
        this.tags = tags;
    }

    public RocketConsumer(RoctetmqUtilsProperties roctetmqUtilsProperties, String tags) {
        this.topic = DEFAULT_TOPIC;
        this.isBroadcasting = DEFAULT_ISBROADCASTING;
        this.tags = tags;
        //消费者的组名
        consumer = new DefaultMQPushConsumer(roctetmqUtilsProperties.getConsumerGroup());
        //指定NameServer地址，多个地址以 ; 隔开
        consumer.setNamesrvAddr(roctetmqUtilsProperties.getNamesrvAddr());
        //订阅PushTopic下Tag为push的消息
        // 订阅topic：myTopic001 下的全部消息（因为是*，*指定的是tag标签，代表全部消息，不进行任何过滤）
        //consumer.subscribe("myTopic001", "*");
        // 这样就只会消费myTopic001下的tag为test-*开头的消息。
        //consumer.subscribe("myTopic001", "test-*");
        try {
            consumer.subscribe(topic, tags);
        } catch (MQClientException e) {
            e.printStackTrace();
        }
        // 代表订阅Topic为myTopic001下的tag为TagA或TagB的所有消息
        //consumer.subscribe("myTopic001", "TagA||TagB");
        //设置Consumer第一次启动是从队列头部开始消费还是队列尾部开始消费
        //如果非第一次启动，那么按照上次消费的位置继续消费
        consumer.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_FIRST_OFFSET);
        //设置消费模式为广播模式，即每个消费者都能接收到所有消息
        if (isBroadcasting) {
            consumer.setMessageModel(MessageModel.BROADCASTING);
        }
    }

    public RocketConsumer(RoctetmqUtilsProperties roctetmqUtilsProperties, String topic, Boolean isBroadcasting, String tags){
        this.topic = topic;
        this.isBroadcasting = isBroadcasting;
        this.tags = tags;
        consumer = new DefaultMQPushConsumer(roctetmqUtilsProperties.getConsumerGroup());
        consumer.setNamesrvAddr(roctetmqUtilsProperties.getNamesrvAddr());
        try {
            consumer.subscribe(topic, tags);
        } catch (MQClientException e) {
            e.printStackTrace();
        }
        consumer.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_FIRST_OFFSET);
        if (isBroadcasting) {
            consumer.setMessageModel(MessageModel.BROADCASTING);
        }
    }

    /**
     * 监听MQ队列，拉取MQ消息并进行消费
     * @param headerInterface   回调方法
     * @throws MQClientException
     * @throws Exception
     */
    public void pullMq(HeaderInterface headerInterface) throws MQClientException, Exception {
        this.pullMq(headerInterface, false);
    }
    /**
     * 监听MQ队列，拉取MQ消息并进行消费(支持广播消费模式)
     * @param headerInterface   回调方法
     * @param isOrder           是否为顺序消息
     * @throws MQClientException
     * @throws Exception
     */
    public void pullMq(HeaderInterface headerInterface, boolean isOrder) throws MQClientException, Exception {
        //消费者的组名
        try {
            if (isOrder){
                //顺序消费
                consumer.registerMessageListener(this.setOrderlyConsumeMessage(headerInterface));
            }else {
                //普通消费
                consumer.registerMessageListener(this.setConcurrentlyConsumeMessage(headerInterface));
            }
            consumer.start();
        } catch (Exception e) {
            logger.error("rocketMq pull msg error:", e);
            throw e;
        } finally {
            this.addShutdownHook(consumer);
        }
    }

    /**
     * 消费端默认消息
     * @param headerInterface
     * @return
     */
    private MessageListenerConcurrently setConcurrentlyConsumeMessage(HeaderInterface headerInterface){
        return (List<MessageExt> list, ConsumeConcurrentlyContext context) -> {
            try {
                for (MessageExt message : list) {
                    headerInterface.execute(message);
                }
            } catch (Exception e) {
                logger.error("rocketMq consumer pull msg error:", e);
                //稍后再试
                return ConsumeConcurrentlyStatus.RECONSUME_LATER;
            }
            //消费成功
            return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
        };
    }

    /**
     * 消费端顺序消息
     * @param headerInterface
     * @return
     */
    private MessageListenerOrderly setOrderlyConsumeMessage(HeaderInterface headerInterface){
        return (list, context) -> {
            try {
                for (MessageExt message : list) {
                    headerInterface.execute(message);
                }
            } catch (Exception e) {
                logger.error("rocketMq consumer pull msg error:", e);
                //稍后再试
                return ConsumeOrderlyStatus.SUSPEND_CURRENT_QUEUE_A_MOMENT;
            }
            //消费成功
            return ConsumeOrderlyStatus.SUCCESS;
        };
    }

    /**
     * 函数式回调接口
     */
    @FunctionalInterface
    public interface HeaderInterface{
        void execute(MessageExt message) throws IOException;
    }

    /**
     * 注册虚拟机器关机钩子事件
     * @param consumer
     */
    private void addShutdownHook(DefaultMQPushConsumer consumer){
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            logger.info("jvm shutdown hook: close rocketMq consumer server ...");
            consumer.shutdown();
        }));
    }
}
