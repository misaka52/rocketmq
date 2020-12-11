package org.apache.rocketmq.test.self;

import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyContext;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.common.message.MessageExt;
import org.junit.Test;

import java.util.List;

/**
 * class description:
 *
 * @author yuanshancheng
 * @date 2020/12/6
 */
public class Consumer {

    @Test
    public void test() throws MQClientException {
        getConsuemr();
    }


    public DefaultMQPushConsumer getConsuemr() throws MQClientException {
        DefaultMQPushConsumer consumer = new DefaultMQPushConsumer(Const.consumerGroup);
        consumer.setNamesrvAddr(Const.nameServer);
        consumer.subscribe(Const.topic, "*");
        consumer.registerMessageListener(new MessageListenerConcurrently() {
            @Override
            public ConsumeConcurrentlyStatus consumeMessage(List<MessageExt> msgs, ConsumeConcurrentlyContext context) {
                for (MessageExt msg : msgs) {
                    System.out.printf("msgId=%s, body=%s,%n", msg.getMsgId(), new String(msg.getBody()));
                }
                return null;
            }
        });
        consumer.start();
        return consumer;
    }
}