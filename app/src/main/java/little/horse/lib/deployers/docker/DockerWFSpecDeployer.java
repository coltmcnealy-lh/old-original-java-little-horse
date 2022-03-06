package little.horse.lib.deployers.docker;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import com.github.dockerjava.api.DockerClient;
import com.github.dockerjava.api.command.CreateContainerCmd;
import com.github.dockerjava.api.command.CreateContainerResponse;
import com.github.dockerjava.api.exception.ConflictException;

import little.horse.common.Config;
import little.horse.common.exceptions.LHConnectionError;
import little.horse.common.exceptions.LHValidationError;
import little.horse.common.objects.metadata.WFSpec;
import little.horse.common.util.Constants;
import little.horse.common.util.LHUtil;
import little.horse.lib.deployers.WorkflowDeployer;

public class DockerWFSpecDeployer implements WorkflowDeployer {
    public void deploy(WFSpec spec, Config config) throws LHConnectionError {

        String containerName = spec.getK8sName();
        DDConfig ddConfig = new DDConfig(); // TODO: Inject the dependency somehow.

        DockerClient client = ddConfig.getDockerClient();

        ArrayList<String> envList = new ArrayList<>();
        HashMap<String, String> env = config.getBaseEnv();
        env.put(Constants.KAFKA_APPLICATION_ID_KEY, spec.name);
        env.put(DDConstants.TASK_DEF_ID_KEY, spec.getId());
        for (Map.Entry<String, String> envEntry: config.getBaseEnv().entrySet()) {
            envList.add(String.format(
                "%s='%s'", envEntry.getKey(), envEntry.getValue())
            );
        }

        try {
            CreateContainerCmd containerCmd = client.createContainerCmd(
                "little-horse-api:latest"
            ).withEnv(
                envList
            ).withName(
                containerName
            ).withEntrypoint( "sleep", "100000" //"java", "-jar", "/littleHorse.jar",
                // "docker-workflow-worker"
            );
            
            containerCmd.getHostConfig().withNetworkMode("host");
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

    public void undeploy(WFSpec spec, Config config) {
        
    }

    public void validate(WFSpec spec, Config config) throws LHValidationError {
        // Nothing to do here, since basically it's just gonna be valid no matter what
    }
    
}