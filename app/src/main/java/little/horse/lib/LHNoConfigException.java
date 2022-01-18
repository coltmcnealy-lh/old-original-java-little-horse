package little.horse.lib;

public class LHNoConfigException extends Exception {
    public String message;

    public LHNoConfigException() {
    }

    public LHNoConfigException(String message) {
        this.message = message;
    }
}
