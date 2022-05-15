package little.horse.deployers.examples.docker;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.github.dockerjava.api.DockerClient;
import com.github.dockerjava.api.command.CreateContainerCmd;
import com.github.dockerjava.api.command.CreateContainerResponse;
import com.github.dockerjava.api.exception.ConflictException;
import com.github.dockerjava.api.model.Container;

import little.horse.common.DepInjContext;
import little.horse.common.exceptions.LHConnectionError;
import little.horse.common.exceptions.LHValidationError;
import little.horse.common.objects.metadata.WFSpec;
import little.horse.common.util.Constants;
import little.horse.common.util.LHUtil;
import little.horse.deployers.WorkflowDeployer;
import little.horse.deployers.examples.common.DeployerConfig;
import little.horse.deployers.examples.common.DeployerConstants;
import little.horse.scheduler.Scheduler;

public class DockerWorkflowDeployer implements WorkflowDeployer {
    public void deploy(WFSpec spec, DepInjContext config) throws LHConnectionError {

        String containerName = spec.getK8sName();
        DeployerConfig ddConfig = new DeployerConfig(); // TODO: Inject the dependency somehow.

        DockerClient client = ddConfig.getDockerClient();

        ArrayList<String> envList = new ArrayList<>();
        HashMap<String, String> env = config.getBaseEnv();
        env.put(Constants.KAFKA_APPLICATION_ID_KEY, spec.name);
        env.put(DeployerConstants.WF_SPEC_ID_KEY, spec.getId());
        env.put(Constants.ADVERTISED_PORT_KEY, "80");
        for (Map.Entry<String, String> envEntry: env.entrySet()) {
            envList.add(String.format(
                "%s=%s", envEntry.getKey(), envEntry.getValue())
            );
        }

        HashMap<String, String> labels = new HashMap<>();
        labels.put("io.littlehorse/deployedBy", "true");
        labels.put("io.littlehorse/wfSpecId", spec.getId());
        labels.put("io.littlehorse/wfSpecName", spec.name);
        labels.put("io.littlehorse/active", "true");

        try {
            CreateContainerCmd containerCmd = client.createContainerCmd(
                "little-horse-api:latest"
            ).withEnv(
                envList
            ).withName(
                containerName
            ).withEntrypoint("java", "-cp", "/littleHorse.jar",
                Scheduler.class.getCanonicalName()
            ).withLabels(labels);

            containerCmd.withHostConfig(
                containerCmd.getHostConfig().withNetworkMode("host")
            );
            CreateContainerResponse container = containerCmd.exec();
    
            client.startContainerCmd(containerName).exec();
            LHUtil.log("Deployed container, got id:", container.getId());
        } catch(ConflictException exn) {
            throw new LHConnectionError(
                exn,
                "Container name " + containerName + " seems to already be taken!"
            );
        } catch(RuntimeException exn) {
            throw new LHConnectionError(exn, "something bad happened!");
        }
    }

    public void undeploy(WFSpec spec, DepInjContext config) {
        DeployerConfig ddConfig = new DeployerConfig(); // TODO: Inject the dependency somehow.

        DockerClient client = ddConfig.getDockerClient();
        HashMap<String, String> labels = new HashMap<>();
        labels.put("io.littlehorse/deployedBy", "true");
        labels.put("io.littlehorse/wfSpecId", spec.getId());
        labels.put("io.littlehorse/wfSpecName", spec.name);

        List<Container> containers = client.listContainersCmd().withLabelFilter(
            labels
        ).exec();

        for (Container cont: containers) {
            client.stopContainerCmd(cont.getId()).exec();
            client.removeContainerCmd(cont.getId()).exec();
        }

    }

    public void validate(WFSpec spec, DepInjContext config) throws LHValidationError {
        // Nothing to do here, since basically it's just gonna be valid no matter what
    }
    
}
