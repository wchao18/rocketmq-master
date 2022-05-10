package com.it.base.producer;

import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.remoting.common.RemotingHelper;

/**
 * 单向发送，将数据写入到客户端的socket缓冲区，可能丢数据
 */
public class OnewayProducer {

    public static void main(String[] args) throws Exception {

        DefaultMQProducer producer = new DefaultMQProducer("one_way");

        producer.setNamesrvAddr("server02:9876;server03:9876");

        producer.start();

        for (int i = 0; i < 5; i++) {
            Message msg = new Message("one_way", "tagB", ("One_Way_Hello RocketMQ " + i).getBytes(RemotingHelper.DEFAULT_CHARSET));
            producer.sendOneway(msg);
        }
        producer.shutdown();
    }
}
