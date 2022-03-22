package little.horse.common.util;

public class LHClassLoadError extends RuntimeException {
    public LHClassLoadError(String message) {
        super(message);
    }

    public LHClassLoadError(String message, Exception exn) {
        super(message, exn);
    }

    public LHClassLoadError() {
        super();
    }
}
