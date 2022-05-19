package little.horse.deployers.examples.docker;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import com.github.dockerjava.api.DockerClient;
import com.github.dockerjava.api.command.CreateContainerCmd;
import com.github.dockerjava.api.command.CreateContainerResponse;

import little.horse.common.DepInjContext;
import little.horse.common.exceptions.LHConnectionError;
import little.horse.common.exceptions.LHSerdeError;
import little.horse.common.exceptions.LHValidationError;
import little.horse.common.objects.BaseSchema;
import little.horse.common.objects.metadata.TaskDef;
import little.horse.common.util.Constants;
import little.horse.common.util.LHClassLoadError;
import little.horse.common.util.LHUtil;
import little.horse.deployers.TaskDeployer;
import little.horse.deployers.examples.common.CustomTaskValidator;
import little.horse.deployers.examples.common.DeployerConfig;
import little.horse.deployers.examples.common.DeployerConstants;
import little.horse.deployers.examples.common.TaskImplTypeEnum;
import little.horse.deployers.examples.common.taskimpl.TaskWorker;


public class DockerTaskDeployer implements TaskDeployer {
    public void deploy(TaskDef spec, DepInjContext config) throws LHConnectionError {
        DeployerConfig ddConfig = new DeployerConfig();
        DockerClient client = ddConfig.getDockerClient();

        DockerTaskDeployMetadata meta;
        try {
            meta = BaseSchema.fromString(
                spec.deployMetadata, DockerTaskDeployMetadata.class, config
            );
        } catch (LHSerdeError exn) {
            throw new RuntimeException("Should be impossible", exn);
        }

        ArrayList<String> envList = new ArrayList<>();
        HashMap<String, String> env = config.getBaseEnv();
        env.put(Constants.KAFKA_APPLICATION_ID_KEY, spec.name);
        env.put(DeployerConstants.TASK_DEF_ID_KEY, spec.getObjectId());
        env.put(DeployerConstants.TASK_EXECUTOR_META_KEY, meta.metadata);
        env.put(DeployerConstants.TASK_EXECUTOR_CLASS_KEY, meta.taskExecutorClassName);

        for (Map.Entry<String, String> envEntry: meta.env.entrySet()) {
            env.put(envEntry.getKey(), envEntry.getValue());
        }

        HashMap<String, String> labels = new HashMap<>();
        labels.put("io.littlehorse/deployedBy", "true");
        labels.put("io.littlehorse/active", "true");
        labels.put("io.littlehorse/taskDefId", spec.getObjectId());

        LHUtil.log("Got labels", labels);

        for (Map.Entry<String, String> envEntry: env.entrySet()) {
            envList.add(String.format(
                "%s=%s", envEntry.getKey(), envEntry.getValue())
            );
        }

        CreateContainerCmd ccc = client.createContainerCmd(
            meta.dockerImage
        ).withEnv(envList).withName(
            "lh-task-" + spec.getObjectId()
        ).withLabels(labels);

        if (meta.taskType == TaskImplTypeEnum.JAVA) {
            ccc = ccc.withCmd(
                "java", "-cp", "/javaInclude:/littleHorse.jar",
                TaskWorker.class.getCanonicalName()
            );
        } else if (meta.taskType == TaskImplTypeEnum.PYTHON) {
            ccc = ccc.withCmd(
                "python", "-m", "executor"
            );
        }

        ccc.withHostConfig(ccc.getHostConfig().withNetworkMode("host"));

        CreateContainerResponse container = ccc.exec();

        LHUtil.log("Deployed container, got id:", container.getId());
        client.startContainerCmd(container.getId()).exec();

        LHUtil.log("Started container!");
    }

    public void undeploy(TaskDef spec, DepInjContext config) throws LHConnectionError {
        DeployerConfig ddconfig = new DeployerConfig();
        DockerClient client = ddconfig.getDockerClient();
        try {
            client.killContainerCmd("lh-task-" + spec.name).exec();
        } catch (Exception exn) {
            // Swallow it so that we end up removing it anyways.
            exn.printStackTrace();
        }
        client.removeContainerCmd("lh-task-" + spec.name).exec();
    }

    public void validate(TaskDef spec, DepInjContext config) throws LHValidationError {
        String message = null;
        if (spec.deployMetadata == null) {
            throw new LHValidationError("Must provide valid Docker validation!");
        }
        try {
            DockerTaskDeployMetadata meta = BaseSchema.fromString(
                spec.deployMetadata, DockerTaskDeployMetadata.class, config
            );
            if (meta.dockerImage == null) {
                message = "Must provide docker image!";
            }

            if (meta.env == null) {
                meta.env = new HashMap<>();
            }

            if (meta.taskType == TaskImplTypeEnum.JAVA) {
                if (meta.taskExecutorClassName == null) {
                    message = "Must provide task executor class for Java tasks!";
                }
            } else if (meta.taskType == TaskImplTypeEnum.PYTHON) {
                if (meta.pythonFunction == null || meta.pythonModule == null) {
                    message = "Must provide module and function for python tasks!";
                }
            }

            if (meta.customValidatorClassName != null) {
                CustomTaskValidator validator = LHUtil.loadClass(
                    meta.customValidatorClassName
                );
                validator.validate(spec, config);
            }

            // The above may have mutated the deployMeta, we want to reflect those
            // changes, so we re-save it.
            spec.deployMetadata = meta.toString();
        } catch (LHSerdeError exn) {
            exn.printStackTrace();
            message = 
                "Failed unmarshalling task deployment metadata: " + exn.getMessage();
        } catch (LHClassLoadError exn) {
            exn.printStackTrace();
            message = "Could not load secondary validator class! " + exn.getMessage();
        }

        if (message != null) {
            throw new LHValidationError(message);
        }
    }
}
