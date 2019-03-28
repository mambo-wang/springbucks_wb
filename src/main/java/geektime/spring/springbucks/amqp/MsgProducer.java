package geektime.spring.springbucks.amqp;

import geektime.spring.springbucks.model.Coffee;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.amqp.rabbit.connection.CorrelationData;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.amqp.support.converter.AbstractJavaTypeMapper;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.time.LocalDateTime;
import java.util.UUID;

@Component
public class MsgProducer{

    private final Logger logger = LoggerFactory.getLogger(this.getClass());

    //由于rabbitTemplate的scope属性设置为ConfigurableBeanFactory.SCOPE_PROTOTYPE，所以不能自动注入
    private RabbitTemplate rabbitTemplate;
    /**
     * 构造方法注入rabbitTemplate
     */
    @Autowired
    public MsgProducer(RabbitTemplate rabbitTemplate) {
        this.rabbitTemplate = rabbitTemplate;
    }

    public void sendMsg(String exchange,String routingKey, String content) {
        CorrelationData correlationId = new CorrelationData(UUID.randomUUID().toString());
        rabbitTemplate.convertAndSend(exchange, routingKey, content, correlationId);
    }

    public void sendMsgWithDirectExchange(String content) {
        CorrelationData correlationId = new CorrelationData(UUID.randomUUID().toString());
        //把消息放入ROUTINGKEY_A对应的队列当中去，对应的是队列A
        rabbitTemplate.convertAndSend(RabbitConfig.EXCHANGE_A, RabbitConfig.ROUTINGKEY_B, content, correlationId);
    }

    public void sendMsgWithTopicExchange(Coffee coffee) {
        this.rabbitTemplate.convertAndSend(RabbitConfig.TOPIC_EXCHANGE, RabbitConfig.ROUTINGKEY_TOPIC, coffee, (message) -> {
            message.getMessageProperties().setHeader(AbstractJavaTypeMapper.DEFAULT_CONTENT_CLASSID_FIELD_NAME, Coffee.class.getName());
            return message;
        });
    }

    public void sendMsgWithFanoutExchange(String content){
        CorrelationData correlationId = new CorrelationData(UUID.randomUUID().toString());
        rabbitTemplate.convertAndSend(RabbitConfig.FANOUT_EXCHANGE, "", content, correlationId);
    }

    public void sendMsgWithNoQueue(String content){
        CorrelationData correlationId = new CorrelationData(UUID.randomUUID().toString());
        rabbitTemplate.convertAndSend(RabbitConfig.EXCHANGE_B,RabbitConfig.ROUTINGKEY_ALL, content, correlationId);
    }

    public void sendMsgWithNoExchange(String content){
        CorrelationData correlationId = new CorrelationData(UUID.randomUUID().toString());
        rabbitTemplate.convertAndSend("sss",RabbitConfig.ROUTINGKEY_ALL, content, correlationId);
    }

    public void sendDeadLetter(String coffee){
        this.rabbitTemplate.convertAndSend(RabbitConfig.EXCHANGE_DELAY, RabbitConfig.ROUTINGKEY_DELAY, coffee, message -> {
            message.getMessageProperties().setHeader(AbstractJavaTypeMapper.DEFAULT_CONTENT_CLASSID_FIELD_NAME, String.class.getName());
            message.getMessageProperties().setExpiration(10 * 1000 + "");
            return message;
        });
        logger.info("延迟消息发送时间：{}", LocalDateTime.now());
    }
}
