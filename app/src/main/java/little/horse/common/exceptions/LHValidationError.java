package little.horse.common.exceptions;

public class LHValidationError extends Exception {
    public LHValidationErrorReason reason = LHValidationErrorReason.INVALID_SPEC;

    public LHValidationError(String errorMessage) {
        super(errorMessage);
    }

    public int getHTTPStatus() {
        switch (reason) {
            case INVALID_SPEC: return 400;
            case REFERS_TO_MISSING_OBJECT: return 404;
            case CONFLICT: return 500;
            default: throw new RuntimeException("Unknown validation failure reason");
        }
    }
}