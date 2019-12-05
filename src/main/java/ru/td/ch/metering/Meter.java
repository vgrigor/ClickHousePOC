package ru.td.ch.metering;

import io.micrometer.core.instrument.DistributionSummary;
import io.micrometer.core.instrument.Metrics;
import io.micrometer.core.instrument.Timer;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import ru.td.ch.util.Application;

import java.util.concurrent.ConcurrentHashMap;

import static java.util.concurrent.TimeUnit.MILLISECONDS;

public class Meter {

    ConcurrentHashMap<String, Integer> meterNames = new ConcurrentHashMap<String, Integer>();

    public static void addTimerMeter(String meterName){

        SimpleMeterRegistry registry = new SimpleMeterRegistry();
        Metrics.addRegistry(registry);

        Timer timer = Metrics.timer(meterName);
    }
    public static void addTimerMeterValue(String meterName, long milliseconds){

       Timer timer = Metrics.timer(meterName);

        timer.record(milliseconds, MILLISECONDS);
    }

    /**
     * Still developing - it's experiment
     */
    public static void addTestMeter(){

        SimpleMeterRegistry registry = new SimpleMeterRegistry();
        DistributionSummary distributionSummary = DistributionSummary
                .builder("request.size")
                .baseUnit("bytes")
                .publishPercentiles(0.5, 0.95)
                .register(registry);

        DistributionSummary distributionSummary1 =
        Metrics.summary("request.1", "bytes", "ss");
        distributionSummary1.record(30);
        distributionSummary1.record(40);
        distributionSummary1.record(50);


        distributionSummary.record(3);
        distributionSummary.record(4);
        distributionSummary.record(5);


        Timer timer = Metrics.timer("app.event");;//registry.timer("app.event");
        timer.record(() -> {
            try {
                MILLISECONDS.sleep(1500);
            } catch (InterruptedException ignored) { }
        });


        timer.record(3000, MILLISECONDS);
        Metrics.addRegistry(registry);



        class CountedObject {
            private CountedObject() {
                Metrics.counter("objects.instance").increment(1.0);
                Metrics.counter("objects.instance").increment(1.0);
                Metrics.counter("objects.instance").increment(1.0);
            }
        }
        Metrics.addRegistry(new SimpleMeterRegistry());

        new CountedObject();

    }

    public static void main(String[] args) {
        addTestMeter();
        Application.waitInfinitely();
    }
}
