package com.it.base.producer;

import lombok.extern.slf4j.Slf4j;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendCallback;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.remoting.common.RemotingHelper;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

/**
 * 异步发送
 */
@Slf4j
public class AsyncProducer {

    public static void main(String[] args) throws Exception {
        DefaultMQProducer producer = new DefaultMQProducer("asyn_test_group_name");

        producer.setInstanceName("生产实例名称-test");

        producer.setNamesrvAddr("server02:9876;server03:9876");

        producer.start();

        //异步失败重试的次数
        producer.setRetryTimesWhenSendAsyncFailed(0);

        int messageCount = 5;
        //异步发送消息,把生产者关了，导致回调报错，因此加countDownLatch
        final CountDownLatch countDownLatch = new CountDownLatch(messageCount);//定义条件

        for (int i = 0; i < messageCount; i++) {
            Message msg = new Message("asyn_test_topic", "tagB", ("Async_Hello RocketMQ9999 " + i).getBytes(RemotingHelper.DEFAULT_CHARSET));
            producer.send(msg, new SendCallback() {
                @Override
                public void onSuccess(SendResult sendResult) {
                    countDownLatch.countDown();
                    log.info("返回结果：" + sendResult);
                }

                @Override
                public void onException(Throwable throwable) {
                    countDownLatch.countDown();
                    log.error("消息发送失败");
                    log.error(throwable.getMessage(), throwable);
                }
            });
        }
        countDownLatch.await(8, TimeUnit.SECONDS);
        producer.shutdown();
    }
}
