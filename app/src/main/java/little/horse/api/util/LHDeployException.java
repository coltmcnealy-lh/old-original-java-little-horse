package little.horse.api.util;

public class LHDeployException extends Exception {
    private String message;
    private int httpStatus;
    public Exception parent;

    public LHDeployException(Exception parent, String message, int httpStatus) {
        this.parent = parent;
        this.httpStatus = httpStatus;
        this.message = message;
    }

    public LHDeployException(String message, int httpStatus) {
        this.message = message;
        this.httpStatus = httpStatus;
    }

    @Override
    public String getMessage() { return message; }

    public int getHttpStatus() { return httpStatus; }
}
