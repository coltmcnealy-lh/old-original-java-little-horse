package little.horse.lib;

public class VarSubOrzDash extends Exception {
    public String message;
    public Exception exn;
    
    public VarSubOrzDash(Exception exn, String message) {
        this.message = message;
        this.exn = exn;
    }
}
