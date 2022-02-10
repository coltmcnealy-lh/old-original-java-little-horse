package little.horse.common.exceptions;

import com.fasterxml.jackson.core.JsonProcessingException;

public class LHSerdeError extends LHException {
    public Exception parent;
    public String message;

    @Override
    public String getMessage() {
        return message;
    }

    public LHSerdeError(Exception parent, String message) {
        this.parent = parent;
        this.message = message;
    }

    public LHSerdeError(JsonProcessingException exn) {
        parent = exn;
        message = "Got a jsonprocessing exception: " + exn.getMessage();
    }

    public LHSerdeError(String message) {
        this.message = message;
    }
}
