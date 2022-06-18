package io.littlehorse.common.objects.metadata;

import java.util.concurrent.Future;

import com.fasterxml.jackson.annotation.JsonIgnore;
import io.littlehorse.common.LHConfig;
import io.littlehorse.common.exceptions.LHConnectionError;
import io.littlehorse.common.exceptions.LHValidationError;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.utils.Bytes;

public abstract class POSTable extends GETable {
    public LHDeployStatus desiredStatus;
    public LHDeployStatus status;
    public String statusMessage;


    // public abstract void processChange(POSTable old) throws LHConnectionError;

    /**
     * Idempotent cleanup of resources when the POSTable is deleted from the API.
     * For example, undeploys the WFRuntime deployer on WFSpec.
     */
    public void remove() throws LHConnectionError {
        // Nothing to do in default.
    }

    @JsonIgnore
    public Future<RecordMetadata> save() {
        ProducerRecord<String, Bytes> record = new ProducerRecord<String, Bytes>(
            getIdKafkaTopic(this.config, this.getClass()),
            getObjectId(),
            new Bytes(this.toBytes())
        );
        return this.config.send(record);
    }

    public abstract void validate(LHConfig config)
    throws LHValidationError, LHConnectionError;
}
