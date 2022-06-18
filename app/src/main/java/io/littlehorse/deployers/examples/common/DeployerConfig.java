package io.littlehorse.deployers.examples.common;

import com.github.dockerjava.api.DockerClient;
import com.github.dockerjava.core.DockerClientBuilder;
import io.littlehorse.common.LHConfig;
import io.littlehorse.common.exceptions.LHConnectionError;
import io.littlehorse.common.objects.metadata.TaskDef;
import io.littlehorse.common.objects.metadata.WFSpec;
import io.littlehorse.common.util.LHDatabaseClient;
import io.littlehorse.common.util.LHUtil;
import io.littlehorse.deployers.examples.common.taskimpl.JavaTask;

public class DeployerConfig {
    private String wfSpecId;
    private String taskDefId;
    private String dockerHost;
    private String taskExecutorClassName;
    private int numThreads;
    
    public DeployerConfig() {
        wfSpecId = System.getenv(DeployerConstants.WF_SPEC_ID_KEY);
        taskDefId = System.getenv(DeployerConstants.TASK_DEF_ID_KEY);
        dockerHost = System.getenv().getOrDefault(
            DeployerConstants.DOCKER_HOST_KEY, "unix:///var/run/docker.sock"
        );
        taskExecutorClassName = System.getenv().get(
            DeployerConstants.TASK_EXECUTOR_CLASS_KEY
        );
        numThreads = Integer.valueOf(System.getenv().getOrDefault(
            DeployerConstants.TASK_EXECUTOR_THREADS_KEY, "10")
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

    public JavaTask getTaskExecutor() {
        return LHUtil.loadClass(taskExecutorClassName);
    }

    public int getNumThreads() {
        return numThreads;
    }

    public String getTaskDefId() {
        return taskDefId;
    }

    public WFSpec lookupWFSpecOrDie(LHConfig config) {
        WFSpec wfSpec = null;
        try {
            wfSpec = LHDatabaseClient.getByNameOrId(
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

    public TaskDef lookupTaskDefOrDie(LHConfig config) {
        TaskDef taskDef = null;
        try {
            taskDef = LHDatabaseClient.getByNameOrId(
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
