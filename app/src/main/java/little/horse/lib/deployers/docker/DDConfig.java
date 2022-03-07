package little.horse.lib.deployers.docker;

import com.github.dockerjava.api.DockerClient;
import com.github.dockerjava.core.DockerClientBuilder;

import little.horse.common.Config;
import little.horse.common.exceptions.LHConnectionError;
import little.horse.common.objects.metadata.TaskDef;
import little.horse.common.objects.metadata.WFSpec;
import little.horse.common.util.LHDatabaseClient;
import little.horse.common.util.LHUtil;
import little.horse.lib.worker.TaskExecutor;

public class DDConfig {
    private String wfSpecId;
    private String taskDefId;
    private String dockerHost;
    private String taskExecutorClassName;
    private int numThreads;
    
    public DDConfig() {
        wfSpecId = System.getenv(DDConstants.WF_SPEC_ID_KEY);
        taskDefId = System.getenv(DDConstants.TASK_DEF_ID_KEY);
        dockerHost = System.getenv().getOrDefault(
            DDConstants.DOCKER_HOST_KEY, "unix:///var/run/docker.sock"
        );
        taskExecutorClassName = System.getenv().get(
            DDConstants.TASK_EXECUTOR_CLASS_KEY
        );
        numThreads = Integer.valueOf(System.getenv().getOrDefault(
            DDConstants.TASK_EXECUTOR_THREADS_KEY, "10")
        );
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

    public TaskExecutor getTaskExecutor() {
        return LHUtil.loadClass(taskExecutorClassName);
    }

    public int getNumThreads() {
        return numThreads;
    }

    public String getTaskDefId() {
        return taskDefId;
    }

    public WFSpec lookupWFSpecOrDie(Config config) {
        WFSpec wfSpec = null;
        try {
            wfSpec = LHDatabaseClient.lookupMetaNameOrId(
                this.getWFSpecId(), config, WFSpec.class
            );
        } catch (LHConnectionError exn) {
            exn.printStackTrace();
        }
        if (wfSpec == null) {
            throw new RuntimeException("Couldn't load wfSpec" + getWFSpecId());
        }
        return wfSpec;
    }

    public TaskDef lookupTaskDefOrDie(Config config) {
        TaskDef taskDef = null;
        try {
            taskDef = LHDatabaseClient.lookupMetaNameOrId(
                this.getTaskDefId(), config, TaskDef.class
            );
        } catch (LHConnectionError exn) {
            exn.printStackTrace();
        }
        if (taskDef == null) {
            throw new RuntimeException("Couldn't load taskDef: " + getTaskDefId());
        }
        return taskDef;

    }

}
