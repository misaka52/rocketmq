package org.apache.rocketmq.test.self;

import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.remoting.exception.RemotingException;
import org.apache.rocketmq.srvutil.ShutdownHookThread;

import java.util.Optional;
import java.util.Scanner;
import java.util.concurrent.Callable;

/**
 * class description:
 *
 * @author yuanshancheng
 * @date 2020/12/6
 */
public class Producer {

    public static void main(String[] args) throws InterruptedException, RemotingException, MQClientException, MQBrokerException {
//        Runtime.getRuntime().addShutdownHook(new Thread(new Runnable() {
//            @Override
//            public void run() {
//                System.out.println("钩子函数执行");
//            }
//        }));
        new Producer().send();
    }

    public void send() throws MQClientException, RemotingException, InterruptedException, MQBrokerException {
        DefaultMQProducer producer = getProducer();

        Scanner scanner = new Scanner(System.in);
        while (scanner.hasNext()) {
            String body = scanner.next();
            Message message = new Message(Const.topic, body.getBytes());
            try {
                producer.send(message);
                System.out.println("发送成功");
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    private DefaultMQProducer getProducer() throws MQClientException {
        DefaultMQProducer producer = new DefaultMQProducer();
        producer.setNamesrvAddr(Const.nameServer);
        producer.setProducerGroup(Const.producerGroup);
        producer.start();
        return producer;
    }
}