package little.horse.common.objects.metadata;

import java.util.HashMap;

import com.fasterxml.jackson.annotation.JsonIgnore;

import org.apache.kafka.clients.admin.NewTopic;

import little.horse.common.Config;
import little.horse.common.exceptions.LHConnectionError;
import little.horse.common.exceptions.LHValidationError;
import little.horse.common.util.LHDatabaseClient;
import little.horse.common.util.LHUtil;
import little.horse.examples.deployers.TaskDeployer;


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
            k8sName = LHUtil.toValidK8sName(getId());
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

    public void processChange(CoreMetadata old) throws LHConnectionError {
        if (old != null && !(old instanceof TaskDef)) {
            throw new RuntimeException(
                "Whatever code made this call is nincompoop."
            );
        }

        TaskDef oldTD = (TaskDef) old;
        TaskDeployer deployer = getTaskDeployer();

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
            if (partitions != null && old.partitions != partitions) {
                throw new LHValidationError(String.format(
                    "Can't change number of partitions from %d to %d!",
                    old.partitions, partitions
                ));
            }
        }

        TaskDeployer deployer = getTaskDeployer();
        deployer.validate(this, config);
    }
}
