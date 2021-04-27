package org.apache.rocketmq.test.self;

import org.apache.rocketmq.common.message.MessageDecoder;
import org.apache.rocketmq.common.message.MessageExt;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import static org.apache.rocketmq.store.ConsumeQueue.CQ_STORE_UNIT_SIZE;

/**
 * @author yuanshancheng
 * @date 2020/12/19
 */
public class Read {
    public static void main(String[] args) throws Exception {
//        readCommitlog();
        readConsumeQueue();
    }

    private static void readCommitlog() throws Exception {
        String path = "/Users/ysc/IdeaProjects/learning/rocketMq/rocketmq/mystore/commitlog/00000000000000000000";
        ByteBuffer buffer = read(path);

        List<MessageExt> msgExtList = new ArrayList<>();
        for (int i = 0; i < 10; ++i) {
            MessageExt msgExt = MessageDecoder.decode(buffer);
            msgExtList.add(msgExt);
            System.out.println(msgExt);
        }
    }

    private static void readConsumeQueue() throws Exception {
        // 0 106108
        // 3 106314
        String path = "/Users/ysc/IdeaProjects/learning/rocketMq/rocketmq/mystore/consumequeue/SELF_TEST_TOPIC2/3/00000000000000000000";
        ByteBuffer buffer = read(path);

        List<MessageExt> msgExtList = new ArrayList<>();
        for (int i = 0; i < 10; ++i) {
            long offset = buffer.getLong();
            int size = buffer.getInt();
            long tagHashCode = buffer.getLong();
            System.out.printf("offset=%d,size=%d,tagHashCode=%d\n", offset, size, tagHashCode);
        }
    }

    private static ByteBuffer read(String path) throws Exception {
        File file = new File(path);
        FileInputStream fileInputStream = new FileInputStream(file);
        byte[] buffer = new byte[(int) file.length()];
        fileInputStream.read(buffer);
        return ByteBuffer.wrap(buffer);
    }
}
