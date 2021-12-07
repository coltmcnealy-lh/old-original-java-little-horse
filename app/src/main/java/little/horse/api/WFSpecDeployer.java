package little.horse.api;

import java.time.Duration;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecords;

import little.horse.lib.Config;
import little.horse.lib.LHDeployError;
import little.horse.lib.LHLookupException;
import little.horse.lib.LHValidationError;
import little.horse.lib.WFSpec.WFSpec;
import little.horse.lib.WFSpec.WFSpecSchema;

public class WFSpecDeployer {
    private Consumer<String, WFSpecSchema> consumer;
    private Config config;

    public WFSpecDeployer(Consumer<String, WFSpecSchema> consumer, Config config) {
        this.consumer = consumer;
        this.config = config;
    }

    public void run() {
        while (true) {
            final ConsumerRecords<String, WFSpecSchema> records = consumer.poll(
                Duration.ofSeconds(1)
            );
            System.out.println(records.count());

            records.forEach(record -> {
                try {
                    System.out.println("hello there");
                    try {
                        Thread.sleep(500);
                    } catch(Exception exn) {}
                    WFSpec spec = WFSpec.fromIdentifier(record.value().guid, config);
                    spec.deploy();
                } catch (LHLookupException exn) {
                    System.out.println("Got a lookup orzdash");
                    System.out.println(exn.getMessage());
                } catch (LHValidationError exn) {
                    System.out.println(exn.getMessage());
                } catch(LHDeployError exn) {
                    System.out.println(exn.getMessage());
                }
            });
        }
    }

    public void shutdown() {
        consumer.close();
    }
}
