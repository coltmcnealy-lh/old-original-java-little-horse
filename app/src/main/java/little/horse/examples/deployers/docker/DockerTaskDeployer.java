package little.horse.examples.deployers.docker;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import com.github.dockerjava.api.DockerClient;
import com.github.dockerjava.api.command.CreateContainerCmd;
import com.github.dockerjava.api.command.CreateContainerResponse;

import little.horse.common.Config;
import little.horse.common.exceptions.LHConnectionError;
import little.horse.common.exceptions.LHSerdeError;
import little.horse.common.exceptions.LHValidationError;
import little.horse.common.objects.BaseSchema;
import little.horse.common.objects.metadata.TaskDef;
import little.horse.common.util.Constants;
import little.horse.common.util.LHClassLoadError;
import little.horse.common.util.LHUtil;
import little.horse.examples.deployers.TaskDeployer;


public class DockerTaskDeployer implements TaskDeployer {
    public void deploy(TaskDef spec, Config config) throws LHConnectionError {
        LHUtil.log("Ok got here");
        DDConfig ddConfig = new DDConfig();
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
        env.put(DDConstants.TASK_DEF_ID_KEY, spec.getId());
        env.put(DDConstants.TASK_EXECUTOR_META_KEY, meta.metadata);
        env.put(DDConstants.TASK_EXECUTOR_CLASS_KEY, meta.taskExecutorClassName);

        HashMap<String, String> labels = new HashMap<>();
        labels.put("io.littlehorse/deployedBy", "true");
        labels.put("io.littlehorse/active", "true");
        labels.put("io.littlehorse/taskDefId", spec.getId());

        LHUtil.log("Got labels", labels);

        for (Map.Entry<String, String> envEntry: env.entrySet()) {
            envList.add(String.format(
                "%s=%s", envEntry.getKey(), envEntry.getValue())
            );
        }

        CreateContainerCmd ccc = client.createContainerCmd(
            meta.dockerImage
        ).withEnv(envList).withName(
            "lh-task-" + spec.getId()
        ).withCmd(
            "java", "-jar", "/littleHorse.jar", "docker-task-worker"
        ).withLabels(labels);

        ccc.withHostConfig(ccc.getHostConfig().withNetworkMode("host"));

        CreateContainerResponse container = ccc.exec();

        LHUtil.log("Deployed container, got id:", container.getId());
        client.startContainerCmd(container.getId()).exec();

        LHUtil.log("Started container!");
    }

    public void undeploy(TaskDef spec, Config config) throws LHConnectionError {
        DDConfig ddconfig = new DDConfig();
        DockerClient client = ddconfig.getDockerClient();

        client.killContainerCmd("lh-task-" + spec.name).exec();
        client.removeContainerCmd("lh-task-" + spec.name).exec();
    }

    public void validate(TaskDef spec, Config config) throws LHValidationError {
        String message = null;
        if (spec.deployMetadata == null) {
            throw new LHValidationError("Must provide valid Docker validation!");
        }
        try {
            DockerTaskDeployMetadata meta = BaseSchema.fromString(
                spec.deployMetadata, DockerTaskDeployMetadata.class, config
            );
            if (meta.dockerImage == null || meta.taskExecutorClassName == null) {
                message = "Must provide docker image and TaskExecutor class name!";
            }

            if (meta.secondaryValidatorClassName != null) {
                DockerSecondaryTaskValidator validator = LHUtil.loadClass(
                    meta.secondaryValidatorClassName
                );
                validator.validate(spec, config);
            }
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
