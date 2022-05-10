package com.it.base.consumer;

import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyContext;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.common.consumer.ConsumeFromWhere;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.remoting.common.RemotingHelper;

import java.util.List;

/**
 * 表面推送，其实是长轮询
 */
@Slf4j
public class SynConsumer {

    public static void main(String[] args) throws MQClientException {
        DefaultMQPushConsumer defaultMQPushConsumer = new DefaultMQPushConsumer("synConsumerGroup");

        defaultMQPushConsumer.setNamesrvAddr("server02:9876;server03:9867");

        defaultMQPushConsumer.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_LAST_OFFSET);

        //tagA或者*
        defaultMQPushConsumer.subscribe("syn_topic", "tagA||tagb");

        //消息监听
        defaultMQPushConsumer.registerMessageListener(new MessageListenerConcurrently() {
            @Override
            public ConsumeConcurrentlyStatus consumeMessage(List<MessageExt> list, ConsumeConcurrentlyContext consumeConcurrentlyContext) {
                MessageExt messageExt = list.get(0);
                try {
                    String topic = messageExt.getTopic();
                    String tags = messageExt.getTags();
                    String keys = messageExt.getKeys();

                    //消息体是字节形式
                    String msgBody = new String(messageExt.getBody(), RemotingHelper.DEFAULT_CHARSET);
                    log.info("消费消息:主题:{},标签:{},keys:{},消息体:{}", topic, tags, keys, msgBody);
                    if (StringUtils.isNotBlank(keys) && keys.equals("keys0")) {
                        throw new Exception();
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                    //记录消息重复消费的次数
                    int reconsumeTimes = messageExt.getReconsumeTimes();
                    if (reconsumeTimes == 3) {
                        return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
                    }
                    return ConsumeConcurrentlyStatus.RECONSUME_LATER;
                }
                return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
            }
        });
        defaultMQPushConsumer.start();
    }
}
