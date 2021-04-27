package org.apache.rocketmq.test.self;

import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyContext;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.common.message.MessageExt;
import org.junit.Test;

import java.util.List;

import static org.apache.rocketmq.common.consumer.ConsumeFromWhere.CONSUME_FROM_FIRST_OFFSET;
import static org.apache.rocketmq.common.consumer.ConsumeFromWhere.CONSUME_FROM_LAST_OFFSET;

/**
 * class description:
 *
 * @author yuanshancheng
 * @date 2020/12/6
 */
public class Consumer {

    @Test
    public void test() throws MQClientException, InterruptedException {
        getConsuemr(Const.consumerGroup5, Const.topic3);
//        getConsuemr(Const.consumerGroup5, Const.topic3);
//        getConsuemr(Const.consumerGroup);
        while (true) {
            Thread.sleep(10000000);
        }
    }

    @Test
    public void test2() throws MQClientException, InterruptedException {
//        getConsuemr(Const.consumerGroup5, Const.topic4);
        getConsuemr(Const.consumerGroup5, Const.topic4);
//        getConsuemr(Const.consumerGroup);
        while (true) {
            Thread.sleep(10000000);
        }
    }


    public DefaultMQPushConsumer getConsuemr(String consumerGroup, String topic) throws MQClientException {
        DefaultMQPushConsumer consumer = new DefaultMQPushConsumer(consumerGroup);
        consumer.setNamesrvAddr(Const.nameServer);
        consumer.subscribe(topic, "*");
        consumer.subscribe(Const.topic5, "*");
        consumer.setConsumeFromWhere(CONSUME_FROM_LAST_OFFSET);
        consumer.setConsumeConcurrentlyMaxSpan(2000);
        consumer.setConsumeTimeout(5);
        consumer.setPullBatchSize(32);
        consumer.setConsumeMessageBatchMaxSize(1);
        consumer.setSuspendCurrentQueueTimeMillis(1000);
        consumer.setConsumeThreadMin(1);
//        consumer.setPullThresholdForQueue(1);
        consumer.setMaxReconsumeTimes(5);
//        consumer.setInstanceName("test");
        consumer.registerMessageListener(new MessageListenerConcurrently() {
            @Override
            public ConsumeConcurrentlyStatus consumeMessage(List<MessageExt> msgs, ConsumeConcurrentlyContext context) {
                System.out.println("开始一次消费，消息数：" + msgs.size() + ",topic=" + msgs.get(0).getTopic());
                for (MessageExt msg : msgs) {
                    System.out.println(msg);
                    System.out.printf("%s\n", msg.getMsgId());
                }
                try {
                    Thread.sleep(1);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
                return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
            }
        });
        consumer.start();
        System.out.println("consumer启动成功");
        return consumer;
    }
}