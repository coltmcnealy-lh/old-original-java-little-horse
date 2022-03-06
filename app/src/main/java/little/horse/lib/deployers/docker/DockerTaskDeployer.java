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
        for (Map.Entry<String, String> envEntry: config.getBaseEnv().entrySet()) {
            envList.add(String.format(
                "%s='%s'", envEntry.getKey(), envEntry.getValue())
            );
        }

        CreateContainerResponse container = client.createContainerCmd(
            meta.dockerImage
        ).withEnv(envList).withName(
            "lh-task-" + spec.getId()
        ).withCmd("--foobar").exec(); // TODO: Figure out how the start command goes.

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
            if (meta.dockerImage == null || meta.escapedCommand == null) {
                message = "Must provide both dockerImage and escapedCommand";
            }
        } catch (IOException exn) {
            exn.printStackTrace();
            message = 
                "Failed unmarshalling task deployment metadata: " + exn.getMessage();
        }

        if (message != null) {
            throw new LHValidationError(message);
        }
    }
}
