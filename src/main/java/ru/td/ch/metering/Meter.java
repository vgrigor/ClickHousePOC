package ru.td.ch.metering;

import io.micrometer.core.instrument.DistributionSummary;
import io.micrometer.core.instrument.Timer;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import ru.td.ch.util.Application;

import java.util.concurrent.TimeUnit;

import static java.util.concurrent.TimeUnit.MILLISECONDS;

public class Meter {

    public static void addTestMeter(){
        SimpleMeterRegistry registry = new SimpleMeterRegistry();
        DistributionSummary distributionSummary = DistributionSummary
                .builder("request.size")
                .baseUnit("bytes")
                .publishPercentiles(0.5, 0.95)
                .register(registry);

        distributionSummary.record(3);
        distributionSummary.record(4);
        distributionSummary.record(5);


        Timer timer = registry.timer("app.event");
        timer.record(() -> {
            try {
                MILLISECONDS.sleep(1500);
            } catch (InterruptedException ignored) { }
        });

        timer.record(3000, MILLISECONDS);

    }


    public static void main(String[] args) {
        addTestMeter();
        Application.waitInfinitely();
    }
}
