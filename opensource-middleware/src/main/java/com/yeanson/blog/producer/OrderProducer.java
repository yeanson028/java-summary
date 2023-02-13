package com.yeanson.blog.producer;

import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendCallback;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.remoting.common.RemotingHelper;

public class OrderProducer {

    private static final String PRODUCER_GROUP_NAME = "ORDER_PRODUCER";

    private static final String NAME_SERVER_ADDR = "192.168.31.92:9876";

    public static void main(String[] args) {
        try {
            //sendSyncProducerMessage();
            sendAsyncProducerMessage();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }


    private static void sendSyncProducerMessage() throws Exception {

        DefaultMQProducer producer = new DefaultMQProducer(PRODUCER_GROUP_NAME);
        producer.setNamesrvAddr(NAME_SERVER_ADDR);
        producer.start();
        for (int i = 0; i < 100; i++) {
            Message message = new Message("OrderTestSyncTopic", "OrderTestTag", ("Hello RocketMQ" + i).getBytes(RemotingHelper.DEFAULT_CHARSET));
            SendResult sendResult = producer.send(message);
            System.out.printf("%s%n", sendResult);
        }

        // 一旦producer不再使用，关闭producer
        producer.shutdown();
    }

    private static void sendAsyncProducerMessage() throws Exception{
        DefaultMQProducer producer = new DefaultMQProducer(PRODUCER_GROUP_NAME);
        producer.setNamesrvAddr(NAME_SERVER_ADDR);
        producer.start();
        producer.setRetryTimesWhenSendAsyncFailed(0);

        for (int i = 0; i < 100; i++) {
            final int index = i;
            Message message = new Message("OrderTestAsyncTopic","OrderTestTag",("Hello World").getBytes(RemotingHelper.DEFAULT_CHARSET));
            producer.send(message, new SendCallback() {
                @Override
                public void onSuccess(SendResult sendResult) {
                    System.out.printf("%-10d OK %s %n", index,
                            sendResult.getMsgId());
                }

                @Override
                public void onException(Throwable e) {
                    System.out.printf("%-10d Exception %s %n", index, e);
                    e.printStackTrace();
                }
            });



        }

    }
}
