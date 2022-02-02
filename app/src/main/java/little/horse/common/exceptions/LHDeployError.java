package little.horse.common.exceptions;

public class LHDeployError extends Exception {
    private String msg;
    
    public LHDeployError(String msg) {
        this.msg = msg;
    }

    public String getMessage() {
        return this.msg;
    }
}
