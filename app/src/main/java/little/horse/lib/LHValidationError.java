package little.horse.lib;

public class LHValidationError extends Exception {
    public LHValidationError(String errorMessage) {
        super(errorMessage);
    }
}