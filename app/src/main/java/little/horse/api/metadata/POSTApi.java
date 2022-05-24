package little.horse.api.metadata;

import java.util.concurrent.ExecutionException;

import org.apache.kafka.clients.producer.RecordMetadata;

import io.javalin.Javalin;
import io.javalin.http.Context;
import little.horse.api.ResponseStatus;
import little.horse.api.util.APIStreamsContext;
import little.horse.common.DepInjContext;
import little.horse.common.exceptions.LHConnectionError;
import little.horse.common.exceptions.LHSerdeError;
import little.horse.common.exceptions.LHValidationError;
import little.horse.common.objects.BaseSchema;
import little.horse.common.objects.metadata.POSTable;
import little.horse.common.util.LHDatabaseClient;
import little.horse.common.util.LHRpcResponse;

public class POSTApi<T extends POSTable> {
    private DepInjContext config;
    private Class<T> cls;
    private APIStreamsContext<T> streamsContext;

    public POSTApi(
        DepInjContext config, Class<T> cls, APIStreamsContext<T> context, Javalin app
    ) {
        this.config = config;
        this.cls = cls;
        this.streamsContext = context;

        // POST /wfSpec
        app.post(T.getAPIPath(cls), this::post);

        // DELETE /wfSpec
        app.delete(T.getAPIPath("{id}", cls), this::delete);
    }


    public void post(Context ctx) {
        LHRpcResponse<T> response = new LHRpcResponse<>();

        try {
            T t = BaseSchema.fromBytes(ctx.bodyAsBytes(), this.cls, config);
            t.validate(config);

            // Absolutely CRUCIAL to call this method before saving it. Otherwise,
            // the random id could be generated twice. TODO: Maybe put this in the
            // BaseSchema.fromBytes() thing?
            t.getObjectId();

            RecordMetadata record = t.save().get();
            streamsContext.waitForProcessing(
                t.getObjectId(), record.offset(), record.partition(), false,
                T.getInternalWaitAPIPath(
                    t.getObjectId(), String.valueOf(record.offset()),
                    String.valueOf(record.partition()), cls
                )
            );

            response.result = LHDatabaseClient.getByNameOrId(t.getObjectId(), config, cls);
            response.objectId = t.getObjectId();
            response.status = ResponseStatus.OK;

        } catch (LHSerdeError exn) {
            exn.printStackTrace();
            response.message = "Failed unmarshaling provided spec: " + exn.getMessage();
            ctx.status(400);
            response.status = ResponseStatus.VALIDATION_ERROR;

        } catch(LHValidationError exn) {
            ctx.status(exn.getHTTPStatus());
            response.message = "Failed validating provided spec: " + exn.getMessage();
            response.status = ResponseStatus.VALIDATION_ERROR;

        } catch(LHConnectionError exn) {
            exn.printStackTrace();
            response.message =
                "Had an internal retriable connection error: " + exn.getMessage();
            response.status = ResponseStatus.INTERNAL_ERROR;
            ctx.status(500);

        } catch(ExecutionException|InterruptedException exn) {
            exn.printStackTrace();
        }

        ctx.json(response);
    }

    public void delete(Context ctx) {
        String id = ctx.pathParam("id");
        LHRpcResponse<T> result = new LHRpcResponse<>();

        try {
            result.result = streamsContext.getTFromId(id, false);
            if (result.result == null) {
                result.status = ResponseStatus.OBJECT_NOT_FOUND;
                result.message = "Could not find " + cls.getTypeName() +
                    " with id " + id;
            } else {
                RecordMetadata record = T.sendNullRecord(id, config, cls).get();
                streamsContext.waitForProcessing(
                    result.result.getObjectId(), record.offset(),
                    record.partition(), false,
                    T.getInternalWaitAPIPath(
                        id, String.valueOf(record.offset()),
                        String.valueOf(record.partition()), cls
                    )
                );
                result.status = ResponseStatus.OK;
                result.message = "Successfully deleted object";
                // TODO: add way to see if the delete actually worked.
            }
        } catch (LHConnectionError exn) {
            result.message = "Failed looking things up: " + exn.getMessage();
            result.status = ResponseStatus.INTERNAL_ERROR;
            ctx.status(500);
        } catch (InterruptedException | ExecutionException exn) {}

        ctx.json(result);
    }
}
