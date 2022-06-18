package io.littlehorse.common.objects.rundata;

public class VarSubOrzDash extends Exception {
    public String message;
    public Exception exn;
    
    public VarSubOrzDash(Exception exn, String message) {
        this.message = message;
        this.exn = exn;
    }

    public String getMessage() {
        return this.message;
    }
}
