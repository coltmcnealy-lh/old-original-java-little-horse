package little.horse.api;

import java.time.Duration;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecords;

import little.horse.lib.Config;
import little.horse.lib.LHDeployError;
import little.horse.lib.LHStatus;
import little.horse.lib.LHUtil;
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
                    WFSpecSchema wfSpec = BaseSchema.fromString(
                        record.value(), WFSpecSchema.class);
                    wfSpec.setConfig(config);
                    if (wfSpec.desiredStatus == LHStatus.REMOVED) {
                        wfSpec.undeploy();
                    } else if (wfSpec.desiredStatus == LHStatus.RUNNING) {
                        wfSpec.deploy();
                    }
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
