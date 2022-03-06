package little.horse.common.objects.metadata;

import java.util.HashMap;

import org.apache.kafka.clients.admin.NewTopic;

import little.horse.common.Config;
import little.horse.common.exceptions.LHConnectionError;
import little.horse.common.exceptions.LHValidationError;
import little.horse.common.util.LHDatabaseClient;
import little.horse.lib.deployers.TaskDeployer;


public class TaskDef extends CoreMetadata {
    public HashMap<String, WFRunVariableDef> requiredVars;
    private Integer partitions = null;

    public int getPartitions() {
        if (partitions == null) {
            return config.getDefaultPartitions();
        }
        return partitions;
    }

    public void setPartitions(int partitions) {
        this.partitions = partitions;
    }

    @Override
    public String getId() {
        return this.name;
    }

    public void setKafkaTopic(String foo) {} // just to make Jackson happy
    public String getKafkaTopic() {
        return this.name;
    }

    /**
     * Used by the TaskDeployer to aid in the deployment of this TaskDef. Can
     * be anything; if the schema is invalid, the TaskDeployer should throw an error
     * in the validate() call.
     * 
     * Recommended use: Serialized JSON.
     */
    public String deployMetadata;

    public void processChange(CoreMetadata old) throws LHConnectionError {
        if (old != null && !(old instanceof TaskDef)) {
            throw new RuntimeException(
                "Whatever code made this call is nincompoop."
            );
        }

        TaskDef oldTD = (TaskDef) old;
        TaskDeployer deployer = config.getTaskDeployer();
    
        if (oldTD != null) {
            if (oldTD.partitions != partitions) {
                throw new RuntimeException("Oh boy, this is bad");
            }

            if (!java.util.Objects.equals(oldTD.deployMetadata, deployMetadata)) {
                deployer.undeploy(oldTD, config);
                deployer.deploy(this, config);
            }
            
        } else {
            config.createKafkaTopic(new NewTopic(
                name, partitions, (short) config.getDefaultReplicas()
            ));
            deployer.deploy(this, config);
        }


    }

    public void validate(Config config) throws LHValidationError, LHConnectionError {
        this.config = config;

        // ALl we gotta do is make sure the TaskDef exists.
        TaskDef old = LHDatabaseClient.lookupMeta(getId(), config, TaskDef.class);

        if (old != null) {
            if (old.partitions != partitions) {
                throw new LHValidationError(String.format(
                    "Can't change number of partitions from %d to %d!",
                    old.partitions, partitions
                ));
            }
        }

        TaskDeployer deployer = config.getTaskDeployer();
        deployer.validate(this, config);
    }
}
