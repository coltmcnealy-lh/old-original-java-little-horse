package io.littlehorse.common.exceptions;

public class VarSubError extends Exception {
    public String message;
    public Exception exn;
    
    public VarSubError(Exception exn, String message) {
        this.message = message;
        this.exn = exn;
    }

    public String getMessage() {
        return this.message;
    }
}
