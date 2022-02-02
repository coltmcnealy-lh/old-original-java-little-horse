package little.horse.common.objects.rundata;

// public enum WFRunStatus {
//     PENDING,
//     RUNNING,
//     COMPLETED,
//     ERROR,
//     PENDING_HALT,
//     HALTED,
//     TIMEOUT,
//     WAITING_FOR_EVENT,
//     WAITING_FOR_LOCK,
//     PENDING_INTERRUPT,
//     INTERRUPTED;
// }

public enum WFRunStatus {
    RUNNING,
    HALTING,
    HALTED,
    COMPLETED;
}
