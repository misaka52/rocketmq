package org.apache.rocketmq.test.self;

import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.LocalTransactionState;
import org.apache.rocketmq.client.producer.TransactionListener;
import org.apache.rocketmq.client.producer.TransactionMQProducer;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.remoting.exception.RemotingException;
import org.apache.rocketmq.srvutil.ShutdownHookThread;
import org.junit.Test;

import java.util.Optional;
import java.util.Scanner;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;

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
            Message message = new Message(Const.topic5, body.getBytes());
            message.setKeys("100001");
            try {
                producer.send(message);
                System.out.println("发送成功");
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    @Test
    public void sendBatch() throws MQClientException, RemotingException, InterruptedException, MQBrokerException {
        DefaultMQProducer producer = getProducer();

        System.out.println("开始发送");
        for (int i = 0; i < 10000; ++i) {
            producer.send(new Message(Const.topic3, ("body" + i).getBytes()));
            if (i % 1000 == 0) {
                System.out.println("已发送" + (i + 1) + "条");
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

    private void transaction() {
        TransactionMQProducer producer = new TransactionMQProducer();
        producer.setNamesrvAddr(Const.nameServer);
        producer.setProducerGroup(Const.producerGroup);
        producer.setTransactionListener(new TransactionListener() {
            @Override
            public LocalTransactionState executeLocalTransaction(Message msg, Object arg) {
                return null;
            }

            @Override
            public LocalTransactionState checkLocalTransaction(MessageExt msg) {
                return null;
            }
        });
    }
}