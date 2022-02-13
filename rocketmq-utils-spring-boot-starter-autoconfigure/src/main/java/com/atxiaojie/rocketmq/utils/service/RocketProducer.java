package com.atxiaojie.rocketmq.utils.service;

import com.atxiaojie.rocketmq.utils.bean.RoctetmqUtilsProperties;
import org.apache.commons.lang3.RandomUtils;
import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.remoting.common.RemotingHelper;
import org.apache.rocketmq.remoting.exception.RemotingException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * @ClassName: RocketProducer
 * @Description: 生产者
 * @author: zhouxiaojie
 * @date: 2021/11/26 11:20
 * @Version: V1.0.0
 */
public class RocketProducer {

    private static Logger logger = LoggerFactory.getLogger(RocketProducer.class);

    private static final String DEFAULT_TOPIC = "defaultTopic";

    private DefaultMQProducer producer;
    private String topic;
    private String tags;
    private String keys;

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

    public String getKeys() {
        return keys;
    }

    public void setKeys(String keys) {
        this.keys = keys;
    }

    public RocketProducer(RoctetmqUtilsProperties roctetmqUtilsProperties) {
        this.topic = DEFAULT_TOPIC;
        //生产者的组名
        producer= new DefaultMQProducer(roctetmqUtilsProperties.getProducerGroup());
        //指定NameServer地址，多个地址以 ; 隔开
        producer.setNamesrvAddr(roctetmqUtilsProperties.getNamesrvAddr());
    }

    public RocketProducer(RoctetmqUtilsProperties roctetmqUtilsProperties, String topic) {
        this.topic = topic;
        //生产者的组名
        producer= new DefaultMQProducer(roctetmqUtilsProperties.getProducerGroup());
        //指定NameServer地址，多个地址以 ; 隔开
        producer.setNamesrvAddr(roctetmqUtilsProperties.getNamesrvAddr());
    }

    public RocketProducer(RoctetmqUtilsProperties roctetmqUtilsProperties, String topic, String tags) {
        this.topic = topic;
        this.tags = tags;
        //生产者的组名
        producer= new DefaultMQProducer(roctetmqUtilsProperties.getProducerGroup());
        //指定NameServer地址，多个地址以 ; 隔开
        producer.setNamesrvAddr(roctetmqUtilsProperties.getNamesrvAddr());
    }

    public RocketProducer(RoctetmqUtilsProperties roctetmqUtilsProperties, String topic, String tags, String keys) {
        this.topic = topic;
        this.tags = tags;
        this.keys = keys;
        //生产者的组名
        producer= new DefaultMQProducer(roctetmqUtilsProperties.getProducerGroup());
        //指定NameServer地址，多个地址以 ; 隔开
        producer.setNamesrvAddr(roctetmqUtilsProperties.getNamesrvAddr());
    }

    /**
     * 发送MQ消息(单个消息可使用此方法)
     * @param msg               消息
     * @return
     * @throws Exception
     */
    public SendResult sendMq(String msg) throws Exception{
        return this.sendMq(Arrays.asList(msg)).get(0);
    }

    /**
     * 发送MQ数组消息
     * @param msgList       消息集合
     * @return
     * @throws MQClientException
     * @throws Exception
     */
    public List<SendResult> sendMq(List<String> msgList) throws MQClientException, Exception {
        return this.sendMq(msgList, false, false, 0);
    }

    /**
     * 发送MQ数组消息
     * @param msgList           消息集合
     * @param isBatch           是否批处理
     * @param isOrder           是否为顺序消息
     * @param delayTimeLevel    消息等级(延后发送)，为0则立即发送，等级从1到10
     * @return
     * @throws MQClientException
     * @throws Exception
     */
    public List<SendResult> sendMq(List<String> msgList, boolean isBatch, boolean isOrder, int delayTimeLevel)
            throws MQClientException, Exception {
        if (msgList == null || msgList.size()<=0){
            throw new RuntimeException("send rocket topic<" + topic + "> mq message to List<String> is null ...");
        }
        List<Message> messageList = new ArrayList<Message>();
        //生成随机数，保证本批消息发送到topic同一个索引位queue，推荐使用具有业务意义的编号，如：订单号，保证同一个订单业务发送到同一个队列中
        int order = RandomUtils.nextInt(1000, 10000);
        //封装到message对象中
        //delayTimeLevel：定时消息等级，指定的时间后才能传递，不支持任意精度时间。按level等级划分，默认从level=1开始,如果level=0则不延时，具体如messageDelayLevel值
        //messageDelayLevel=1s 5s 10s 30s 1m 2m 3m 4m 5m 6m 7m 8m 9m 10m 20m 30m 1h 2h
        if(tags == null && keys == null){
            for (String msg : msgList){
                Message message = new Message(topic, msg.getBytes(RemotingHelper.DEFAULT_CHARSET));
                message.setDelayTimeLevel(delayTimeLevel);
                messageList.add(message);
                logger.info("send message body:{}", message.toString());
            }
        }else if(tags != null && keys == null){
            for (String msg : msgList){
                Message message = new Message(topic, tags, msg.getBytes(RemotingHelper.DEFAULT_CHARSET));
                message.setDelayTimeLevel(delayTimeLevel);
                messageList.add(message);
                logger.info("send message body:{}", message.toString());
            }
        }else if(tags != null && keys != null){
            for (String msg : msgList){
                Message message = new Message(topic, tags, keys, msg.getBytes(RemotingHelper.DEFAULT_CHARSET));
                message.setDelayTimeLevel(delayTimeLevel);
                messageList.add(message);
                logger.info("send message body:{}", message.toString());
            }
        }
        try {
            producer.start();
            //是否批量发送，批量发送消息可提高传递小消息的性能
            if (isBatch) {
                return this.sendMqAlikeList(producer, messageList, isOrder, order);
            }else {
                return this.sendMqDisaffinityList(producer, messageList, isOrder, order);
            }
        }  catch (MQClientException mqce) {
            logger.error("send rocket topic<" + topic + "> rocketMq client error:", mqce);
            throw mqce;
        }  catch (Exception e) {
            logger.error("send rocket topic<" + topic + "> rocketMq send msg error:", e);
            throw e;
        } finally {
            //关闭生产者
            producer.shutdown();
        }
    }

    /**
     * 快速或顺序发送消息
     * @param producer
     * @param messageList
     * @param isOrder
     * @param order
     * @return
     * @throws Exception
     */
    private List<SendResult> sendMqDisaffinityList(DefaultMQProducer producer, List<Message> messageList, boolean isOrder, int order){
        List<SendResult> sendResultList = new ArrayList<SendResult>();
        for (Message m : messageList){
            try {
                if (isOrder){
                    //顺序发送，适用场景：订单的下单、待支付、已支付、待打包、已发货等，一定要保证消息顺序消费
                    //对所有队例总数取模计算，获得索引位上的队列
                    sendResultList.add(producer.send(m, (list, message, o) -> list.get((Integer) o % list.size()), order));
                }else {
                    sendResultList.add(producer.send(m));
                }
            } catch (MQClientException e) {
                e.printStackTrace();
            } catch (RemotingException e) {
                e.printStackTrace();
            } catch (MQBrokerException e) {
                e.printStackTrace();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        return sendResultList;
    }

    /**
     * 批量或批量顺序发送消息
     * 使用限制:同一批次的消息应具有：相同的主题，相同的waitStoreMsgOK，并且不支持计划。一批消息的总大小不得超过1M
     * @param producer
     * @param messageList
     * @param isOrder
     * @param order
     * @return
     * @throws Exception
     */
    private List<SendResult> sendMqAlikeList(DefaultMQProducer producer, List<Message> messageList, boolean isOrder, int order) throws Exception{
        if (isOrder){
            //顺序发送，适用场景：订单的下单、待支付、已支付、待打包、已发货等，一定要保证消息顺序消费
            //注意：如果新的topic，此处获取topic下的队列会出错，请先建立topic
            List<MessageQueue> list = producer.fetchPublishMessageQueues(topic);
            //对所有队例总数取模计算，获得索引位上的队列
            return Arrays.asList(producer.send(messageList, list.get(order % list.size())));
        }else {
            return Arrays.asList(producer.send(messageList));
        }
    }
}
