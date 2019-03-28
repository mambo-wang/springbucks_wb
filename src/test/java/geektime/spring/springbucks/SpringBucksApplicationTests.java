package geektime.spring.springbucks;

import geektime.spring.springbucks.amqp.MsgProducer;
import geektime.spring.springbucks.model.Coffee;
import org.joda.money.CurrencyUnit;
import org.joda.money.Money;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;

import java.util.stream.IntStream;

@RunWith(SpringRunner.class)
@SpringBootTest
public class SpringBucksApplicationTests {

	@Autowired
	private MsgProducer msgProducer;

	/**
	 * 测试returnCallback，当消息没有正确到达队列时触发回调方法
	 */
	@Test
	public void sendMsgWithNoQueue() {
		msgProducer.sendMsgWithNoQueue("hello");

	}

	/**
	 * 测试confirmCallback，消息没有提交到交换机的时候触发
	 */
	@Test
	public void noExchange() {
		msgProducer.sendMsgWithNoExchange("hello");

	}

	/**
	 * 测试directExchange
	 */
	@Test
	public void testACK() {
		msgProducer.sendMsgWithDirectExchange("helloa");
	}

	/**
	 * 测试topicExchange
	 */
	@Test
	public void topic() {
		IntStream.rangeClosed(1,5).forEach(i->{
			msgProducer.sendMsgWithTopicExchange(new Coffee());
		});
	}

	/**
	 * 测试fanoutExchange
	 */
	@Test
	public void fanout() {
		IntStream.rangeClosed(1,5).forEach(i->{
			msgProducer.sendMsgWithFanoutExchange("fanout-----" + i);
		});
	}

	/**
	 * 测试延迟消息
	 */
	@Test
	public void delay() {
		msgProducer.sendDeadLetter("coffee");
	}
}

