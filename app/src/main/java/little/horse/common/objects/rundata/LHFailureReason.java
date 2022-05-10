package little.horse.common.objects.rundata;

public enum LHFailureReason {
    TASK_FAILURE,
    VARIABLE_LOOKUP_ERROR,
    INVALID_WF_SPEC_ERROR,
    TIMEOUT,
    SUBTHREAD_FAILURE,
    INTERNAL_LITTLEHORSE_ERROR;
}
