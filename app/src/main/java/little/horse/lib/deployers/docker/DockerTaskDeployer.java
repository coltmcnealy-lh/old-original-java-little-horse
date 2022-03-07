package little.horse.lib.deployers.docker;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.dockerjava.api.DockerClient;
import com.github.dockerjava.api.command.CreateContainerResponse;
import com.github.dockerjava.core.DockerClientBuilder;

import little.horse.common.Config;
import little.horse.common.exceptions.LHConnectionError;
import little.horse.common.exceptions.LHValidationError;
import little.horse.common.objects.metadata.TaskDef;
import little.horse.common.util.Constants;
import little.horse.common.util.LHClassLoadError;
import little.horse.common.util.LHUtil;
import little.horse.lib.deployers.TaskDeployer;


public class DockerTaskDeployer implements TaskDeployer {
    public void deploy(TaskDef spec, Config config) throws LHConnectionError {
        DockerClient client = DockerClientBuilder.getInstance(
            "tcp://host.docker.internal:2375"
        ).build();

        DockerTaskDeployMetadata meta;
        try {
            meta = new ObjectMapper().readValue(
                spec.deployMetadata, DockerTaskDeployMetadata.class
            );
        } catch (IOException exn) {
            throw new RuntimeException("Should be impossible", exn);
        }

        ArrayList<String> envList = new ArrayList<>();
        HashMap<String, String> env = config.getBaseEnv();
        env.put(Constants.KAFKA_APPLICATION_ID_KEY, spec.name);
        env.put(DDConstants.TASK_DEF_ID_KEY, spec.getId());
        env.put(DDConstants.TASK_EXECUTOR_META_KEY, meta.metadata);
        env.put(DDConstants.TASK_EXECUTOR_CLASS_KEY, meta.taskExecutorClassName);

        for (Map.Entry<String, String> envEntry: config.getBaseEnv().entrySet()) {
            envList.add(String.format(
                "%s=%s", envEntry.getKey(), envEntry.getValue())
            );
        }

        CreateContainerResponse container = client.createContainerCmd(
            meta.dockerImage
        ).withEnv(envList).withName(
            "lh-task-" + spec.getId()
        ).withCmd(
            "java", "-jar", "/littleHorse.jar", "docker-task-worker"
        ).exec();

        LHUtil.log("Deployed container, got id:", container.getId());
        client.startContainerCmd(container.getId()).exec();
    }

    public void undeploy(TaskDef spec, Config config) throws LHConnectionError {
        DockerClient client = DockerClientBuilder.getInstance(
            "tcp://host.docker.internal:2375"
        ).build();

        client.killContainerCmd("lh-task-" + spec.name).exec();
    }

    public void validate(TaskDef spec, Config config) throws LHValidationError {
        String message = null;
        try {
            DockerTaskDeployMetadata meta = new ObjectMapper().readValue(
                spec.deployMetadata, DockerTaskDeployMetadata.class
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
        } catch (IOException exn) {
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
