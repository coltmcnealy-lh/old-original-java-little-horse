package little.horse.lib;

public enum LHStatus {
    PENDING,
    SCHEDULED,
    STARTING,
    RUNNING,
    COMPLETED,
    WAITING_FOR_EVENT,
    STOPPED,
    ERROR,
    PENDING_REMOVAL,
    REMOVED;
}
