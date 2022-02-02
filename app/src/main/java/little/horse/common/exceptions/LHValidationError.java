package little.horse.common.exceptions;

public class LHValidationError extends Exception {
    public LHValidationError(String errorMessage) {
        super(errorMessage);
    }
}