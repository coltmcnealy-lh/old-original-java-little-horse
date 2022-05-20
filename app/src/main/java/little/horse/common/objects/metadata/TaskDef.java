package little.horse.common.objects.metadata;

import java.util.HashMap;

import com.fasterxml.jackson.annotation.JsonIgnore;

import org.apache.kafka.clients.admin.NewTopic;

import little.horse.common.DepInjContext;
import little.horse.common.exceptions.LHConnectionError;
import little.horse.common.exceptions.LHValidationError;
import little.horse.common.util.LHDatabaseClient;
import little.horse.common.util.LHUtil;
import little.horse.deployers.TaskDeployer;


public class TaskDef extends POSTable {
    public HashMap<String, WFRunVariableDef> requiredVars;
    private Integer partitions = null;

    public int versionNumber = 0;

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
    public String getObjectId() {
        return this.name;
    }

    public void setKafkaTopic(String foo) {} // just to make Jackson happy
    public String getKafkaTopic() {
        return this.name;
    }

    private String taskDeployerClassName;
    public String getTaskDeployerClassName() {
        if (taskDeployerClassName == null) {
            taskDeployerClassName = config.getDefaultTaskDeployerClassName();
        }
        return taskDeployerClassName;
    }

    private String k8sName;
    public String getK8sName() {
        if (k8sName == null) {
            k8sName = LHUtil.toValidK8sName(getObjectId());
        }
        return k8sName;
    }
    /**
     * Just there for Jackson.
     * @param foo NO TOUCHY TOUCHY!
     */
    public void setK8sName(String foo) {}

    /**
     * Should only be called by Jackson...humans, don't call this!
     * @param foo NO TOUCHY TOUCHY!
     */
    public void setTaskDeployerClassName(String foo) {
        taskDeployerClassName = foo;
    }

    /**
     * Used by the TaskDeployer to aid in the deployment of this TaskDef. Can
     * be anything; if the schema is invalid, the TaskDeployer should throw an error
     * in the validate() call.
     * 
     * Recommended use: Serialized JSON.
     */
    public String deployMetadata;

    @JsonIgnore
    public TaskDeployer getTaskDeployer() {
        return LHUtil.loadClass(getTaskDeployerClassName());
    }

    public void processChange(POSTable old) throws LHConnectionError {
        if (old != null && !(old instanceof TaskDef)) {
            throw new RuntimeException(
                "Whatever code made this call is nincompoop."
            );
        }

        TaskDef oldTD = (TaskDef) old;
        TaskDeployer deployer = getTaskDeployer();

        if (oldTD != null) {
            versionNumber = oldTD.versionNumber + 1;

            if (oldTD.partitions != partitions) {
                throw new RuntimeException(
                    "Oh boy, this is bad and shouldn't be possible"
                );
            }

            if (
                !java.util.Objects.equals(oldTD.deployMetadata, deployMetadata) ||
                desiredStatus == LHDeployStatus.DESIRED_REDEPLOY
            ) {
                deployer.undeploy(oldTD, config);
                deployer.deploy(this, config);
                this.status = LHDeployStatus.RUNNING;
                this.desiredStatus = LHDeployStatus.RUNNING;
            }

        } else {
            config.createKafkaTopic(new NewTopic(
                name, partitions, (short) config.getDefaultReplicas()
            ));
            deployer.deploy(this, config);
            this.status = LHDeployStatus.RUNNING;
        }

    }

    @Override
    @JsonIgnore
    public void remove() throws LHConnectionError {
        getTaskDeployer().undeploy(this, config);
        this.status = LHDeployStatus.STOPPED;
    }

    public void validate(DepInjContext config) throws LHValidationError, LHConnectionError {
        this.config = config;

        // ALl we gotta do is make sure the TaskDef exists.
        TaskDef old = LHDatabaseClient.getById(getObjectId(), config, TaskDef.class);

        if (old != null) {
            if (partitions != null && old.partitions != partitions) {
                throw new LHValidationError(String.format(
                    "Can't change number of partitions from %d to %d!",
                    old.partitions, partitions
                ));
            }
        }

        TaskDeployer deployer;
        try {
            deployer = getTaskDeployer();
        } catch(Exception exn) {
            throw new LHValidationError(
                "Failed loading Task Deployer Class: " + exn.getMessage()
            );
        }
        deployer.validate(this, config);
    }
}
