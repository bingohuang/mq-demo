package com.netease.fund.nqs;

import com.netease.cloud.nqs.client.ClientConfig;
import com.netease.cloud.nqs.client.Message;
import com.netease.cloud.nqs.client.MessageSessionFactory;
import com.netease.cloud.nqs.client.SimpleMessageSessionFactory;
import com.netease.cloud.nqs.client.producer.MessageProducer;
import com.netease.cloud.nqs.client.producer.ProducerConfig;

/**
 * 实现描述：nqs消息生产者
 *
 * @author bingo 2015/11/6, 17:01
 */
public class Producer {

    public static void main(String[] argv) throws Exception {
        ClientConfig clientConfig = new ClientConfig();
//        clientConfig.setHost("10.165.147.163");
        clientConfig.setHost("10.165.124.180");
//        clientConfig.setHost("10.165.124.181");
        clientConfig.setPort(5672);
        clientConfig.setProductId("ab0a0ce6f39a40e5b0a2625f343735e5");
        clientConfig.setAccessKey("26db5f5aecd345f0b77325c4b975d8a0");
        clientConfig.setAccessSecret("cc03cf791deb4f85af3d0667f6530eba");

        MessageSessionFactory simpleMessageSessionFactory = new SimpleMessageSessionFactory(clientConfig);

        ProducerConfig producerConfig = new ProducerConfig();
        producerConfig.setProductId("ab0a0ce6f39a40e5b0a2625f343735e5");
        producerConfig.setQueueName("wylc-dev");

        MessageProducer producer = simpleMessageSessionFactory.createProducer(producerConfig);

        // 根据需求更改这里的循环条件
        while (true) {
            Message message = new Message("hello world bingo".getBytes());

            try {
                producer.sendMessage(message);
                break;
            } catch (Exception e) {
                if (producer != null) {
                    try {
                        producer.shutdown();
                    } catch (Exception e1) {
                        // ignore
                    }
                }

                // 根据需求更改这里的等待时间
                Thread.sleep(5000);

                producer = simpleMessageSessionFactory.createProducer(producerConfig);
            } finally {
                if (producer != null) {
                    try {
                        producer.shutdown();
                    } catch (Exception e1) {
                        // ignore
                    }
                }
            }
        }
    }
}
