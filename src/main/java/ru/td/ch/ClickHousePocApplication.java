package ru.td.ch;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.event.ContextRefreshedEvent;
import org.springframework.context.event.EventListener;
import org.springframework.scheduling.annotation.EnableAsync;
import ru.td.ch.config.CmdlArgs;
import ru.td.ch.metering.Meter;
import ru.td.ch.repository.Addresses;
import ru.td.ch.util.Application;

import javax.annotation.PostConstruct;
import java.sql.SQLException;

@SpringBootApplication
@ComponentScan("ru.td.ch")
public class ClickHousePocApplication {

	public static void main(String[] args) throws Exception {
		SpringApplication.run(ClickHousePocApplication.class, args);

		Meter.addTestMeter();

		CmdlArgs.setup(args);

		Addresses.doLoadData();
	}

}
