package little.horse.lib;

import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.Response;

import java.io.IOException;
import java.util.ArrayList;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import org.apache.kafka.clients.producer.ProducerRecord;

public class TaskDef {
    private Config config;
    private TaskDefSchema schema;

    public TaskDef(TaskDefSchema schema, Config config) throws LHValidationError {
        // TODO (hard): do some validation that we don't have a 409.
        if (schema.guid == null) {
            schema.guid = LHUtil.generateGuid();
        }

        if (schema.bashCommand == null) {
            throw new LHValidationError(
                "No bash command provided on the task definition schema."
            );
        }

        if (schema.name == null) {
            throw new LHValidationError(
                "No name provided on the task definition schema."
            );
        }

        if (schema.dockerImage == null) {
            schema.dockerImage = config.getDefaultTaskDockerImage();
        }

        this.schema = schema;
        this.config = config;
    }

    public static TaskDef fromIdentifier(String identifier, Config config) throws LHLookupException {
        OkHttpClient client = config.getHttpClient();
        String url = config.getAPIUrlFor(Constants.TASK_DEF_API_PATH) + "/" + identifier;
        Request request = new Request.Builder().url(url).build();
        Response response;
        String responseBody = null;

        try {
            response = client.newCall(request).execute();
            responseBody = response.body().string();
        }
        catch (IOException exn) {
            String err = "Got an error making request to " + url + ": " + exn.getMessage() + ".\n";
            err += "Was trying to call URL " + url;

            System.err.println(err);
            throw new LHLookupException(exn, LHLookupExceptionReason.IO_FAILURE, err);
        }

        // Check response code.
        if (response.code() == 404) {
            throw new LHLookupException(
                null,
                LHLookupExceptionReason.OBJECT_NOT_FOUND,
                "Could not find TaskDef with identifier " + identifier + "."
            );
        } else if (response.code() != 200) {
            if (responseBody == null) {
                responseBody = "";
            }
            throw new LHLookupException(
                null,
                LHLookupExceptionReason.OTHER_ERROR,
                "API Returned an error: " + String.valueOf(response.code()) + " " + responseBody
            );
        }

        TaskDefSchema schema = null;
        try {
            schema = new ObjectMapper().readValue(responseBody, TaskDefSchema.class);
        } catch (JsonProcessingException exn) {
            System.err.println(
                "Got an invalid response: " + exn.getMessage() + " " + responseBody
            );
            throw new LHLookupException(
                exn,
                LHLookupExceptionReason.INVALID_RESPONSE,
                "Got an invalid response: " + responseBody + " " + exn.getMessage()
            );
        }

        try {
            return new TaskDef(schema, config);
        } catch (LHValidationError exn) {
            System.err.println("Orzdash we shouldn't be able to get here.");
            // Shouldn't be possible because in order for the thing to get into the
            // datastore, it had to have already passed this validation.
            throw new LHLookupException(exn, LHLookupExceptionReason.OTHER_ERROR, "Orzdash");
        }
    }

    public TaskDefSchema getModel() {
        return this.schema;
    }

    public String toString() {
        ObjectMapper mapper = new ObjectMapper();
        String result;
        try {
            result = mapper.writeValueAsString(this.getModel());
        } catch(JsonProcessingException exn) {
            System.out.println(exn.toString());
            result = "Could not serialize.";
        }
        return result;
    }

    public ArrayList<String> getTaskDaemonCommand() {
        return config.getTaskDaemonCommand();
    }

    public void record() {
        ProducerRecord<String, String> record = new ProducerRecord<String, String>(
            this.config.getTaskDefTopic(),
            schema.guid,
            this.toString()
        );
        this.config.send(record);
    }
}