package little.horse.common.exceptions;

public class LHConnectionError extends Exception {
    private Exception parent;
    private LHLookupExceptionReason reason;
    private String message;

    public LHConnectionError(Exception exn, LHLookupExceptionReason rsn, String msg) {
        this.message = msg;
        this.reason = rsn;
        this.parent = exn;
    }

    public String getMessage() {
        return this.message;
    }
    
    public LHLookupExceptionReason getReason() {
        return this.reason;
    }

    public Exception parent() {
        return this.parent;
    }
}