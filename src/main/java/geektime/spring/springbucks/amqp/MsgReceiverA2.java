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

@Component
@RabbitListener(queues = RabbitConfig.QUEUE_A)
public class MsgReceiverA2 {

    private final Logger logger = LoggerFactory.getLogger(this.getClass());


    @RabbitHandler
    public void process(String content, Channel channel, Message message) throws IOException {
        logger.info("消费者A2---接收处理队列A当中的消息： " + content);

        try {
            channel.basicAck(message.getMessageProperties().getDeliveryTag(), false);
            logger.info("------------------4 receiver success------------------");
        } catch (IOException e) {
            if(message.getMessageProperties().getRedelivered()){
                logger.error("4 重复消费，拒绝接收");
                channel.basicReject(message.getMessageProperties().getDeliveryTag(), false);//拒绝消息
            }else {
                logger.error("4 返回队列处理");
                channel.basicNack(message.getMessageProperties().getDeliveryTag(), false,true);
            }
        }
    }

}
