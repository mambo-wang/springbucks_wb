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
@RabbitListener(queues = RabbitConfig.QUEUE_A)
public class MsgReceiverA {

    private final Logger logger = LoggerFactory.getLogger(this.getClass());


    @RabbitHandler
    public void process(String content, Channel channel, Message message) throws IOException {
        logger.info("消费者A---接收处理队列A当中的消息： {}, 当前时间为: {}", content, LocalDateTime.now());

        try {
            //multiple：当该参数为 true 时，则可以一次性确认 delivery_tag 小于等于传入值的所有消息
            //false时，仅确认提供的delivery_tag的消息
            // channel.basicAck(message.getMessageProperties().getDeliveryTag(),false);
            channel.basicAck(message.getMessageProperties().getDeliveryTag(), false);
            logger.info("------------------1 receiver success------------------");
//            throw new Exception("商品失效");
        } catch (Exception e) {
            e.printStackTrace();
            logger.info("------------------------1 receiver fail--------------------");
            if(message.getMessageProperties().getRedelivered()){
                logger.error("重复消费，拒绝接收");
                //requeue 当该参数为 true 时，如果被拒绝的消息应该被重新请求，而不是丢弃/死字符
                channel.basicReject(message.getMessageProperties().getDeliveryTag(), false);//拒绝消息
            }else {
                //multiple 当该参数为 true 时，则可以一次性Nack delivery_tag 小于等于传入值的所有消息
                //false时，仅Nack提供的delivery_tag的消息
                //requeue 当该参数为 true 时，如果被Nack的消息应该被重新请求，而不是丢弃/死字符
                logger.error("返回队列处理");
                channel.basicNack(message.getMessageProperties().getDeliveryTag(), false,true);
            }
        }
    }

}
