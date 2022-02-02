package little.horse.common.objects.rundata;

/**
 * Enum used to denote the reason why a ThreadRun or WFRun is in the HALTING or
 * HALTED state. Stored in a collection so that if a thread is halted for multiple
 * reasons (i.e. thread was interrupted AND user stopped the workflow), and one
 * reason is resolved (i.e. user resumes the workflow), we know whether or not to
 * resume the thread (in this case, not yet because the thread is still interrupted)
 */
public enum WFHaltReasonEnum {
    PARENT_STOPPED,
    PARENT_INTERRUPTED,
    FAILED,
    INTERRUPT,
    HANDLING_EXCEPTION,
    MANUAL_STOP;    
}
