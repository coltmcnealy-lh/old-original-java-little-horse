package little.horse.lib.deployers.docker;

import com.github.dockerjava.api.DockerClient;
import com.github.dockerjava.core.DockerClientBuilder;

public class DDConfig {
    private String wfSpecId;
    private String dockerHost;
    
    public DDConfig() {
        wfSpecId = System.getenv(DDConstants.WF_SPEC_ID_KEY);
        dockerHost = System.getenv(DDConstants.DOCKER_HOST_KEY);
        dockerHost = (dockerHost == null)
            ? "tcp://host.docker.internal:2375" : dockerHost;
    }

    public String getDockerHost() {
        return this.dockerHost;
    }

    public String getWFSpecId() {
        return this.wfSpecId;
    }

    public DockerClient getDockerClient() {
        return DockerClientBuilder.getInstance(this.getDockerHost()).build();
    }
}
