package little.horse;

import org.apache.kafka.clients.consumer.KafkaConsumer;

import little.horse.lib.Config;
import little.horse.lib.WFRun.WFRunSchema;
import little.horse.lib.WFSpec.NodeSchema;
import little.horse.lib.WFSpec.WFSpec;

public class DaemonAppDaemon {
    private Config config;
    private NodeSchema node;
    private WFSpec wfSpec;
    private KafkaConsumer<String, WFRunSchema> consumer;

    public DaemonAppDaemon(Config config, WFSpec wfSpec, NodeSchema node) {
        this.config = config;
        this.wfSpec = wfSpec;
        this.node = node;
    }

    public void run() {
        
    }
}
