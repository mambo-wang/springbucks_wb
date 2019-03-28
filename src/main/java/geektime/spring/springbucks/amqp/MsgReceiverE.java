package geektime.spring.springbucks.amqp;

import com.rabbitmq.client.Channel;
import geektime.spring.springbucks.model.Coffee;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.rabbit.annotation.RabbitHandler;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.time.LocalDateTime;

@Component
@RabbitListener(queues = RabbitConfig.QUEUE_E)
public class MsgReceiverE {

    private final Logger logger = LoggerFactory.getLogger(this.getClass());


    @RabbitHandler
    public void process(Coffee content, Channel channel, Message message) throws IOException {
        logger.info("消费者E---接收处理topic.*当中的消息： {}, 当前时间：{}", content.toString(), LocalDateTime.now());

        try {
            channel.basicAck(message.getMessageProperties().getDeliveryTag(), false);
            logger.info("------------------3 receiver success------------------");
        } catch (IOException e) {
            e.printStackTrace();
            //丢弃这条消息
            channel.basicNack(message.getMessageProperties().getDeliveryTag(), false, true);
            logger.info("------------------------3 receiver fail--------------------");
        }
    }

}
