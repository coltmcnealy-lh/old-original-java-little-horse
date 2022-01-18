package little.horse.api;

import java.time.Duration;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecords;

import little.horse.lib.Config;
import little.horse.lib.LHDeployError;
import little.horse.lib.LHLookupException;
import little.horse.lib.LHStatus;
import little.horse.lib.LHUtil;
import little.horse.lib.LHValidationError;
import little.horse.lib.objects.WFSpec;
import little.horse.lib.schemas.BaseSchema;
import little.horse.lib.schemas.WFSpecSchema;

public class WFSpecDeployer {
    private Consumer<String, String> consumer;
    private Config config;

    public WFSpecDeployer(Consumer<String, String> consumer, Config config) {
        this.consumer = consumer;
        this.config = config;
    }

    public void run() {
        while (true) {
            final ConsumerRecords<String, String> records = consumer.poll(
                Duration.ofSeconds(1)
            );

            records.forEach(record -> {
                try {
                    try {
                        Thread.sleep(500);
                    } catch(Exception exn) {}
                    WFSpecSchema schema = BaseSchema.fromString(record.value(), WFSpecSchema.class);
                    WFSpec spec = WFSpec.fromIdentifier(schema.guid, config);
                    if (schema.desiredStatus == LHStatus.REMOVED) {
                        spec.undeploy();
                    } else if (schema.desiredStatus == LHStatus.RUNNING) {
                        spec.deploy();
                    }
                } catch (LHLookupException exn) {
                    exn.printStackTrace();
                    LHUtil.log("Got a lookup orzdash", exn.getMessage());
                exn.printStackTrace();} catch (LHValidationError exn) {
                    exn.printStackTrace();
                    LHUtil.log("Got a validation orzdash", exn.getMessage());
                } catch(LHDeployError exn) {
                    exn.printStackTrace();
                    LHUtil.log("Got a deploy orzdash", exn.getMessage());
                }
            });
        }
    }

    public void shutdown() {
        consumer.close();
    }
}
