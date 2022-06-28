package io.littlehorse.common.exceptions;

public class LHSerdeError extends RuntimeException {
    public Exception exn;
    public String msg;

    public LHSerdeError(String msg) {
        this.msg = msg;
    }

    public LHSerdeError(String message, Exception parent) {
        this.exn = parent;
        this.msg = message;
    }

    public String getMessage() {
        String out = msg;
        if (exn != null) {
            out += "\n" + exn.getMessage();
        }
        return out;
    }
}
