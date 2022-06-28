package io.littlehorse.common.model.metadata;

public enum LHDeployStatus {
    STARTING,
    RUNNING,
    COMPLETED,
    STOPPING,
    STOPPED,
    DESIRED_REDEPLOY,
    ERROR;
}
