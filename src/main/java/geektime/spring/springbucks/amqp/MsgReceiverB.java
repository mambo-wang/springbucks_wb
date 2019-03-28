package geektime.spring.springbucks.amqp;

import com.rabbitmq.client.Channel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.rabbit.annotation.RabbitHandler;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.stereotype.Component;

import java.io.IOException;

@Component
@RabbitListener(queues = RabbitConfig.QUEUE_B)
public class MsgReceiverB {

    private final Logger logger = LoggerFactory.getLogger(this.getClass());

    @RabbitHandler
    public void process(String content, Channel channel, Message message) throws IOException {
        logger.info("消费者B---接收处理队列B当中的消息： " + content);

        try {
            channel.basicAck(message.getMessageProperties().getDeliveryTag(), false);
            logger.info("------------------2 receiver success------------------");
        } catch (IOException e) {
            e.printStackTrace();
            //丢弃这条消息
            channel.basicNack(message.getMessageProperties().getDeliveryTag(), false, true);
            logger.info("------------------------2 receiver fail--------------------");
        }
    }

}
