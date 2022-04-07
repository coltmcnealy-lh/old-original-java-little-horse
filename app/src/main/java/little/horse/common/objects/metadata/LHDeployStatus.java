package little.horse.common.objects.metadata;

public enum LHDeployStatus {
    STARTING,
    RUNNING,
    COMPLETED,
    STOPPING,
    STOPPED,
    DESIRED_REDEPLOY,
    ERROR;
}
