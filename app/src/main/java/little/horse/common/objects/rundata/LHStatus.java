package little.horse.common.objects.rundata;

public enum LHStatus {
    PENDING,
    SCHEDULED,
    STARTING,
    RUNNING,
    COMPLETED,
    FAILED_AND_HANDLED,
    WAITING_FOR_EVENT,
    STOPPED,
    ERROR,
    PENDING_REMOVAL,
    REMOVED;
}
