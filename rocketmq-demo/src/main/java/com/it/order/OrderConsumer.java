package com.it.order;

import lombok.extern.slf4j.Slf4j;
import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.ConsumeOrderlyContext;
import org.apache.rocketmq.client.consumer.listener.ConsumeOrderlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerOrderly;
import org.apache.rocketmq.common.message.MessageExt;

import java.util.List;

/**
 * @author wangchao
 * @description 顺序消费者
 * @date 2020/04/14 15:00
 */
@Slf4j
public class OrderConsumer {

    public static void main(String[] args) throws Exception {
        //1.创建消费者Consumer，制定消费者组名
        DefaultMQPushConsumer consumer = new DefaultMQPushConsumer("orderGroup");
        //2.指定Nameserver地址
        consumer.setNamesrvAddr("server02:9876;server03:9876");
        //3.订阅主题Topic和Tag
        consumer.subscribe("OrderTopic", "*");

        //4.注册消息监听器
        consumer.registerMessageListener(new MessageListenerOrderly() {

            @Override
            public ConsumeOrderlyStatus consumeMessage(List<MessageExt> msgs, ConsumeOrderlyContext context) {
                for (MessageExt msg : msgs) {
                    log.info("线程名称：【" + Thread.currentThread().getName() + "】:" + new String(msg.getBody()));
                }
                return ConsumeOrderlyStatus.SUCCESS;
            }
        });

        //5.启动消费者
        consumer.start();

        log.info("消费者启动");

    }
}
