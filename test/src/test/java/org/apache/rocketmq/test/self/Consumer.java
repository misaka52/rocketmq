package org.apache.rocketmq.test.self;

import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyContext;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.common.message.MessageExt;
import org.junit.Test;

import java.util.List;

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
        getConsuemr(Const.consumerGroup);
        while (true) {
            Thread.sleep(10000000);
        }
    }


    public DefaultMQPushConsumer getConsuemr(String consumerGroup) throws MQClientException {
        DefaultMQPushConsumer consumer = new DefaultMQPushConsumer(consumerGroup);
        consumer.setNamesrvAddr(Const.nameServer);
        consumer.subscribe(Const.topic, "*");
        consumer.setConsumeFromWhere(CONSUME_FROM_LAST_OFFSET);
        consumer.setConsumeConcurrentlyMaxSpan(2000);
        consumer.setConsumeTimeout(5);
        consumer.setPullBatchSize(32);
        consumer.setConsumeMessageBatchMaxSize(1);
        consumer.setSuspendCurrentQueueTimeMillis(1000);
        consumer.setConsumeThreadMin(10);
//        consumer.setPullThresholdForQueue(1);
        consumer.setMaxReconsumeTimes(10);
        consumer.registerMessageListener(new MessageListenerConcurrently() {
            @Override
            public ConsumeConcurrentlyStatus consumeMessage(List<MessageExt> msgs, ConsumeConcurrentlyContext context) {
                System.out.println("开始一次消费，消息数：" + msgs.size());
                for (MessageExt msg : msgs) {
                    System.out.println("msgId=" + msg.getMsgId() + "  body:" + new String(msg.getBody()));
                    System.out.println(msg);
//                    System.out.printf("msgId=%s, %s", msg.getMsgId(), msg);
                }
                return ConsumeConcurrentlyStatus.RECONSUME_LATER;
            }
        });
        consumer.start();
        return consumer;
    }
}