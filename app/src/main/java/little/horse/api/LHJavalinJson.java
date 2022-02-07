package little.horse.api;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;

import com.fasterxml.jackson.core.JsonProcessingException;

import org.jetbrains.annotations.NotNull;

import io.javalin.plugin.json.JsonMapper;
import little.horse.common.Config;
import little.horse.common.objects.BaseSchema;
import little.horse.common.util.LHUtil;

public class LHJavalinJson implements JsonMapper {
    private Config config;

    public LHJavalinJson(Config config) {
        this.config = config;
    }

    @NotNull()
    @Override
    public String toJsonString(@NotNull() Object obj) {
        try {
            return LHUtil.mapper.writeValueAsString(obj);
        } catch (JsonProcessingException exn) {
            exn.printStackTrace();
            return null;
        }
    }

    @NotNull()
    @Override
    public InputStream toJsonStream(@NotNull() Object obj) {
        try {
            byte[] bytes = LHUtil.mapper.writeValueAsBytes(obj);
            return new ByteArrayInputStream(bytes);
        } catch (JsonProcessingException exn) {
            exn.printStackTrace();
            return null;
        }
    }

    @Override
    @NotNull()
    public <T> T fromJsonString(@NotNull() String json, @NotNull() Class<T> targetClass) {
        if (BaseSchema.class.isAssignableFrom(targetClass)) {

            @SuppressWarnings("unchecked")
            Class<? extends BaseSchema> jedi = (Class<? extends BaseSchema>)targetClass;
            return BaseSchema.fromString(json, jedi, config, false);

        } else {
            try {
                return LHUtil.mapper.readValue(json, targetClass);
            } catch (JsonProcessingException exn) {
                exn.printStackTrace();
                return null;
            }
        }
    }

    @Override
    @NotNull()
    public <T> T fromJsonStream(@NotNull() InputStream json, @NotNull() Class<T> targetClass) {
        try {
            if (BaseSchema.class.isAssignableFrom(targetClass)) {

                @SuppressWarnings("unchecked")
                Class<? extends BaseSchema> jedi = (Class<? extends BaseSchema>)targetClass;
                return BaseSchema.fromBytes(json.readAllBytes(), jedi, config, false);

            } else {
                try {
                    return LHUtil.mapper.readValue(json, targetClass);
                } catch (JsonProcessingException exn) {
                    exn.printStackTrace();
                    return null;
                }
            }
        } catch (IOException exn) {
            exn.printStackTrace();
            return null;
        }
    }
}
