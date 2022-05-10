package com.it.base.producer;

import lombok.extern.slf4j.Slf4j;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.remoting.common.RemotingHelper;

/**
 * 同步发送
 */
@Slf4j
public class SynProducer {

    public static void main(String[] args) throws Exception {

        DefaultMQProducer producer = new DefaultMQProducer("syn_group_name0706");

        producer.setNamesrvAddr("server02:9876;server03:9876");

        //同步发送失败重试次数
        producer.setRetryTimesWhenSendFailed(2);

        producer.start();

        for (int i = 0; i < 5; i++) {
            Message msg = new Message("syn_topic", "tagA", "keys" + i, ("Hello RocketMQ " + i).getBytes(RemotingHelper.DEFAULT_CHARSET));
            SendResult sendResult = producer.send(msg);
            log.info("返回结果：" + sendResult);
        }
        producer.shutdown();


    }
}
