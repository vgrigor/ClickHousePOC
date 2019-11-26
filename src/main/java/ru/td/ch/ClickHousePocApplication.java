package ru.td.ch;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.event.ContextRefreshedEvent;
import org.springframework.context.event.EventListener;
import org.springframework.scheduling.annotation.EnableAsync;
import ru.td.ch.config.BeanUtil;
import ru.td.ch.repository.Addresses;

import javax.annotation.PostConstruct;
import java.sql.SQLException;
import java.util.concurrent.CompletableFuture;

@EnableAsync
@SpringBootApplication
@ComponentScan("ru.td.ch")
public class ClickHousePocApplication {

	public static void main(String[] args) throws Exception {
		SpringApplication.run(ClickHousePocApplication.class, args);

		Addresses.doLoadData();

	}

	@PostConstruct
	void init() throws SQLException {

	}

	@EventListener
	public void handleContextRefreshEvent(ContextRefreshedEvent ctxStartEvt) throws SQLException {
		System.out.println("Context Start Event received.");

/*		if(true)
		return;

		//Addresses a = new Addresses().setUp().runTest();
		Addresses a = BeanUtil.getBean(Addresses.class).setUp().runTest();


		CompletableFuture< Long > f1 = a.GenerateLoadStream(100_000_000);
		//CompletableFuture< Long > f2 = a.GenerateStream(3_000_000);
		//CompletableFuture< Long > f3 = a.GenerateStream(4_000_000);

		//CompletableFuture.allOf(f1, f2, f3).join();
		CompletableFuture.allOf(f1).join();

		System.out.println("=================================");
		System.out.println("ВСЕ ОТПРАВЛЕНО");*/


	}







}
