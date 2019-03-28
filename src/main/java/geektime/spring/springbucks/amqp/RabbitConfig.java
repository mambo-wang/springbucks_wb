package geektime.spring.springbucks.amqp;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.amqp.core.*;
import org.springframework.amqp.rabbit.connection.CachingConnectionFactory;
import org.springframework.amqp.rabbit.connection.ConnectionFactory;
import org.springframework.amqp.rabbit.connection.CorrelationData;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.amqp.rabbit.listener.SimpleMessageListenerContainer;
import org.springframework.amqp.rabbit.listener.api.ChannelAwareMessageListener;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Scope;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

@Configuration
public class RabbitConfig {

    private final Logger logger = LoggerFactory.getLogger(this.getClass());

    @Value("${spring.rabbitmq.host}")
    private String host;

    @Value("${spring.rabbitmq.port}")
    private int port;

    @Value("${spring.rabbitmq.username}")
    private String username;

    @Value("${spring.rabbitmq.password}")
    private String password;


    public static final String EXCHANGE_A = "my-mq-exchange_A";
    public static final String EXCHANGE_B = "my-mq-exchange_B";
    public static final String EXCHANGE_C = "my-mq-exchange_C";

    public static final String FANOUT_EXCHANGE="fanout_exchange";
    public static final String TOPIC_EXCHANGE="topic_exchange";
    public static final String EXCHANGE_DELAY="exchange.delay";


    public static final String QUEUE_A = "QUEUE_A";
    public static final String QUEUE_B = "QUEUE_B";
    public static final String QUEUE_C = "QUEUE_C";
    public static final String QUEUE_D = "QUEUE_D";
    public static final String QUEUE_E = "QUEUE_E";

    public static final String QUEUE_DELAY = "queue.delay";

    public static final String ROUTINGKEY_A = "spring-boot-routingKey_A";
    public static final String ROUTINGKEY_B = "spring-boot-routingKey_B";
    public static final String ROUTINGKEY_C = "spring-boot-routingKey_C";
    public static final String ROUTINGKEY_ALL = "spring-boot-routingKey_ALL";
    public static final String ROUTINGKEY_TOPIC = "topic.message.test";
    public static final String ROUTINGKEY_DELAY = "routing.delay";



    @Bean
    public ConnectionFactory connectionFactory() {
        CachingConnectionFactory connectionFactory = new CachingConnectionFactory(host,port);
        connectionFactory.setUsername(username);
        connectionFactory.setPassword(password);
        connectionFactory.setVirtualHost("/");
        connectionFactory.setPublisherConfirms(true);
        connectionFactory.setPublisherReturns(true);
        return connectionFactory;
    }

    @Bean
    @Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
    //必须是prototype类型
    public RabbitTemplate rabbitTemplate() {
        RabbitTemplate template = new RabbitTemplate(connectionFactory());
        template.setMandatory(true);
        template.setConfirmCallback(this::confirm);
        template.setReturnCallback(this::returnedMessage);
        return template;
    }

    /**
     * 针对消费者配置
     * 1. 设置交换机类型
     * 2. 将队列绑定到交换机
     FanoutExchange: 将消息分发到所有的绑定队列，无routingkey的概念
     HeadersExchange ：通过添加属性key-value匹配
     DirectExchange: 按照routingkey分发到指定队列
     TopicExchange: 多关键字匹配
     */
    @Bean
    public DirectExchange defaultExchange() {
        return new DirectExchange(EXCHANGE_A);
    }

    @Bean
    public DirectExchange exchangeC() {
        return new DirectExchange(EXCHANGE_C);
    }


    /**
     * 获取队列A
     * @return
     */
    @Bean
    public Queue queueA(){
        return new Queue(QUEUE_A, true);//队列持久化
    }

    /**
     * 获取队列B
     * @return
     */
    @Bean
    public Queue queueB(){
        return new Queue(QUEUE_B, true);//队列持久化
    }

    /**
     * 获取队列C
     * @return
     */
    @Bean
    public Queue queueC(){
        return new Queue(QUEUE_C, true);//队列持久化
    }

    @Bean
    public Queue queueD(){
        return new Queue(QUEUE_D, true);//队列持久化
    }

    @Bean
    public Queue queueE(){
        return new Queue(QUEUE_E, true);//队列持久化
    }

    /**延迟队列配置*/
    @Bean
    public Queue delayProcessQueue(){
        Map<String, Object> params = new HashMap<>();
        params.put("x-dead-letter-exchange", EXCHANGE_C);
        params.put("x-dead-letter-routing-key", ROUTINGKEY_C);
        params.put("x-message-ttl", 5000);
        return new Queue(QUEUE_DELAY, true, false, false, params);

    }

    @Bean
    public Binding binding(){
        return BindingBuilder.bind(queueA()).to(defaultExchange()).with(RabbitConfig.ROUTINGKEY_A);
    }

    @Bean
    public Binding bindingB(){
        return BindingBuilder.bind(queueB()).to(defaultExchange()).with(RabbitConfig.ROUTINGKEY_B);
    }

    @Bean
    public Binding bindingC(){
        return BindingBuilder.bind(queueC()).to(exchangeC()).with(RabbitConfig.ROUTINGKEY_C);
    }

    /**
     * 接收多个队列中的消息
     * @return
     */
    @Bean
    public SimpleMessageListenerContainer messageContainer() {
        //加载处理消息A的队列
        SimpleMessageListenerContainer container = new SimpleMessageListenerContainer(connectionFactory());
        //设置接收多个队列里面的消息，这里设置接收队列A
        //假如想一个消费者处理多个队列里面的信息可以如下设置：
        container.setQueues(queueD());
//        container.setQueues(queueA());
        container.setExposeListenerChannel(true);
        //设置最大的并发的消费者数量
        container.setMaxConcurrentConsumers(10);
        //最小的并发消费者的数量
        container.setConcurrentConsumers(1);
        //设置确认模式手工确认
        container.setAcknowledgeMode(AcknowledgeMode.MANUAL);
        container.setMessageListener((ChannelAwareMessageListener) (message, channel) -> {
            try {
                /**通过basic.qos方法设置prefetch_count=1，这样RabbitMQ就会使得每个Consumer在同一个时间点最多处理一个Message，
                 换句话说,在接收到该Consumer的ack前,它不会将新的Message分发给它 */
                channel.basicQos(1);
                byte[] body = message.getBody();
                logger.info("接收处理队列当中的消息:" + message.getMessageProperties() + "：" + new String(body));
                logger.info("-----------topic:" + message.getMessageProperties().getReceivedRoutingKey());
                /**为了保证永远不会丢失消息，RabbitMQ支持消息应答机制。
                 当消费者接收到消息并完成任务后会往RabbitMQ服务器发送一条确认的命令，然后RabbitMQ才会将消息删除。*/
                channel.basicAck(message.getMessageProperties().getDeliveryTag(), false);
            } catch (IOException e) {
                e.printStackTrace();
                if(message.getMessageProperties().getRedelivered()){
                    logger.error("重复消费，拒绝接收");
                    channel.basicReject(message.getMessageProperties().getDeliveryTag(), false);//拒绝消息
                }else {
                    logger.error("返回队列处理");
                    channel.basicNack(message.getMessageProperties().getDeliveryTag(), false,true);
                }
            }
        });
        return container;
    }

    @Bean
    public DirectExchange delayExchange(){
        return new DirectExchange(EXCHANGE_DELAY);
    }

    @Bean
    public Binding dlxBinding(){
        return BindingBuilder.bind(delayProcessQueue()).to(delayExchange()).with(ROUTINGKEY_DELAY);
    }

    @Bean
    FanoutExchange fanoutExchange(){
        return new FanoutExchange(RabbitConfig.FANOUT_EXCHANGE);
    }

    /**
     * 绑定所有队列到交换机
     * @param queueA
     * @param fanoutExchange
     * @return
     */
    @Bean
    Binding bindingExchangeA(Queue queueA, FanoutExchange fanoutExchange){
        return BindingBuilder.bind(queueA).to(fanoutExchange);
    }

    @Bean
    Binding bindingExchangeB(Queue queueB, FanoutExchange fanoutExchange){
        return BindingBuilder.bind(queueB).to(fanoutExchange);
    }

    @Bean
    Binding bindingExchangeC(Queue queueC, FanoutExchange fanoutExchange){
        return BindingBuilder.bind(queueC).to(fanoutExchange);
    }


    @Bean
    TopicExchange topicExchange(){
        return new TopicExchange(RabbitConfig.TOPIC_EXCHANGE);
    }

    @Bean
    Binding bindTopicD(){
        return BindingBuilder.bind(queueD()).to(topicExchange()).with(ROUTINGKEY_TOPIC);
    }
    @Bean
    Binding bindTopicE(){
        return BindingBuilder.bind(queueE()).to(topicExchange()).with("topic.*");
    }



    /**
     * 只确认消息是否正确到达Exchange中
     */
    public void confirm(CorrelationData correlationData, boolean ack, String cause) {
        if (ack) {
            logger.info("消息成功消费");
        } else {
            logger.info("消息消费失败:" + cause);
        }
        logger.info("消息唯一标识："+correlationData);
        logger.info("确认结果："+ack);

    }

    /**
     * 消息没有正确到达队列时触发回调，如果正确到达队列不执行
     * @param message
     * @param replyCode
     * @param replyText
     * @param exchange
     * @param routingKey
     */
    public void returnedMessage(Message message, int replyCode, String replyText, String exchange, String routingKey) {
        logger.info("消息主体 message : "+message);
        logger.info("消息主体 message : "+replyCode);
        logger.info("描述："+replyText);
        logger.info("消息使用的交换器 exchange : "+exchange);
        logger.info("消息使用的路由键 routing : "+routingKey);
    }

}
