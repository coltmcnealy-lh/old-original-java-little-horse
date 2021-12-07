package little.horse.lib;

public class LHDeployError extends Exception {
    private String msg;
    
    public LHDeployError(String msg) {
        this.msg = msg;
    }

    public String getMessage() {
        return this.msg;
    }
}
