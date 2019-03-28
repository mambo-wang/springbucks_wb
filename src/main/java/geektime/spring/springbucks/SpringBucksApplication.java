package geektime.spring.springbucks;

import geektime.spring.springbucks.amqp.MsgProducer;
import geektime.spring.springbucks.model.Coffee;
import geektime.spring.springbucks.service.CoffeeService;
import lombok.extern.slf4j.Slf4j;
import org.joda.money.CurrencyUnit;
import org.joda.money.Money;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.cache.annotation.EnableCaching;
import org.springframework.data.jpa.repository.config.EnableJpaRepositories;
import org.springframework.transaction.annotation.EnableTransactionManagement;

@Slf4j
@EnableTransactionManagement
@SpringBootApplication
@EnableJpaRepositories
@EnableCaching(proxyTargetClass = true)
public class SpringBucksApplication implements ApplicationRunner {
	@Autowired
	private CoffeeService coffeeService;

	@Autowired
	private MsgProducer msgProducer;

	public static void main(String[] args) {
		SpringApplication.run(SpringBucksApplication.class, args);
	}

	@Override
	public void run(ApplicationArguments args) throws Exception {


		Coffee coffee = Coffee.builder().name("latte").price(Money.of(CurrencyUnit.USD, 10)).build();
		msgProducer.sendMsgWithTopicExchange(coffee);

		/******************测试redis cache****************/
//		log.info("Count: {}", coffeeService.findAllCoffee().size());
//		for (int i = 0; i < 5; i++) {
//			log.info("Reading from cache.");
//			coffeeService.findAllCoffee();
//		}
////		Thread.sleep(5_000);
//		log.info("Reading after refresh.");
//		coffeeService.findAllCoffee().forEach(c -> log.info("Coffee {}", c.getName()));
//
//		System.out.println("-------------------------------------------");
//		coffeeService.findByName("latte");
//		for (int i = 0; i < 5; i++) {
//			log.info("find by name, Reading from cache.");
//			coffeeService.findByName("latte");
//		}
//
//		Coffee coffee = coffeeService.findByName("capuccino");
//		coffeeService.delete(coffee);
//
//		System.out.println("-------------------------------------------");
//
//		coffeeService.save(coffee);

//		coffeeService.reloadCoffee();


	}
}

