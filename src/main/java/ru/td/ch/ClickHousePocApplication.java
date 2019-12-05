package ru.td.ch;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Profile;
import org.springframework.context.event.ContextRefreshedEvent;
import org.springframework.context.event.EventListener;
import org.springframework.scheduling.annotation.EnableAsync;
import ru.td.ch.config.CmdlArgs;
import ru.td.ch.config.spring.YAMLConfig;
import ru.td.ch.metering.Meter;
import ru.td.ch.repository.Addresses;
import ru.td.ch.util.Application;

import javax.annotation.PostConstruct;
import java.sql.SQLException;

@SpringBootApplication
@ComponentScan("ru.td.ch")
public class ClickHousePocApplication {

	@Autowired
	private YAMLConfig myConfig;

	public static void main(String[] args) throws Exception {
		CmdlArgs.setup(args);
		SpringApplication.run(ClickHousePocApplication.class, args);

	}

	@PostConstruct
	void init() throws Exception {
		myConfig.run(myConfig);

		Addresses.doLoadData();
	}

}
