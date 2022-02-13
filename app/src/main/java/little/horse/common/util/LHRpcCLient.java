package little.horse.common.util;

import java.io.IOException;

import little.horse.common.Config;
import little.horse.common.exceptions.LHConnectionError;
import little.horse.common.exceptions.LHSerdeError;
import little.horse.common.objects.metadata.CoreMetadata;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.Response;

public class LHRpcCLient {
    private Config config;

    public LHRpcCLient(Config config) {
        this.config = config;
    }

    public<T extends CoreMetadata> LHRpcResponse<T> getResponse(
        String url, Class<T> cls
    ) throws LHConnectionError {
        byte[] response = getResponse(url);

        try {
            return LHRpcResponse.fromResponse(response, config, cls);

        } catch (LHSerdeError exn) {
            throw new RuntimeException(
                "If we get this, it means that some invalid JSON got thrown into " +
                "RocksDB, which shouldn't be possible. Colt should be fired ;) "
            );

        }
    }

    private byte[] getResponse(String url) throws LHConnectionError {
        OkHttpClient client = config.getHttpClient();
        Request request = new Request.Builder().url(url).build();

        try {
            Response resp = client.newCall(request).execute();
            byte[] body = resp.body().bytes();
            if (resp.code() < 300 && resp.code() >= 200) {
                return body;
            } else {
                throw new LHConnectionError(
                    null,
                    String.format("Got a %s response from API: %s", resp.code(), body)
                );
            }
        } catch(IOException exn) {
            // java.net.ConnectException is included in IOException.
            // java.net.SocketTimeoutException also included in IOException.
            throw new LHConnectionError(
                exn,
                String.format(
                    "Had %s error connectiong to %s: %s",
                    exn.getClass().getName(), url, exn.getMessage()
                )
            );
        }
    }
}
