package com.netease.fund.nqs;

import com.netease.cloud.nqs.client.ClientConfig;
import com.netease.cloud.nqs.client.Message;
import com.netease.cloud.nqs.client.MessageSessionFactory;
import com.netease.cloud.nqs.client.SimpleMessageSessionFactory;
import com.netease.cloud.nqs.client.consumer.ConsumerConfig;
import com.netease.cloud.nqs.client.consumer.MessageConsumer;
import com.netease.cloud.nqs.client.consumer.MessageHandler;

/**
 * 实现描述：nqs消息消费者
 *
 * @author bingo 2015/11/6, 17:13
 */
public class Consumer {
    public static void main(String[] argv) throws Exception {
        ClientConfig clientConfig = new ClientConfig();
        clientConfig.setHost("10.165.124.181");
        clientConfig.setPort(5672);
        clientConfig.setProductId("ab0a0ce6f39a40e5b0a2625f343735e5");
        clientConfig.setAccessKey("26db5f5aecd345f0b77325c4b975d8a0");
        clientConfig.setAccessSecret("cc03cf791deb4f85af3d0667f6530eba");

        MessageSessionFactory simpleMessageSessionFactory = new SimpleMessageSessionFactory(clientConfig);

        ConsumerConfig consumerConfig = new ConsumerConfig();
        consumerConfig.setProductId("ab0a0ce6f39a40e5b0a2625f343735e5");
        consumerConfig.setQueueName("wylc-dev");
        consumerConfig.setRequireAck(true);
        consumerConfig.setGroup("wylc_consumer_group");

        MessageConsumer consumer = null;

        // 根据需求更改这里的循环条件
        while (true) {
            try {
                consumer = simpleMessageSessionFactory.createConsumer(consumerConfig);
                consumer.consumeMessage(new MessageHandler() {
                    @Override
                    public boolean handle(Message message) {
                        //根据需求更改这里的消息处理逻辑
                        System.out.println("received: " + new String(message.getBody()));
                        return true;
                    }
                });
            } catch (Exception e) {
                if (consumer != null) {
                    try {
                        consumer.shutdown();
                    } catch (Exception e1) {
                        // ignore
                    }
                }

                // 根据需求更改这里的等待时间
                Thread.sleep(5000);

                consumer = simpleMessageSessionFactory.createConsumer(consumerConfig);
            } finally {
                if (consumer != null) {
                    try {
                        consumer.shutdown();
                    } catch (Exception e1) {
                        // ignore
                    }
                }
            }
        }
    }
}
