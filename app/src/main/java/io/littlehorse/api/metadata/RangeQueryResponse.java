package io.littlehorse.api.metadata;

import java.util.ArrayList;
import java.util.Base64;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.fasterxml.jackson.annotation.JsonIgnore;
import io.littlehorse.common.LHConfig;
import io.littlehorse.common.objects.BaseSchema;
import io.littlehorse.common.util.LHUtil;

public class RangeQueryResponse extends BaseSchema {
    public List<String> objectIds;

    @JsonIgnore
    public Map<String, String> partitionBookmarks;

    public String getToken() {
        return RangeQueryResponse.bookmarkMapToString(partitionBookmarks);
    }

    public void setToken(String token) {
        partitionBookmarks = RangeQueryResponse.tokenToBookmarkMap(token, config);
    }

    @JsonIgnore
    public String lowestKeySeen() {
        return lowestKeySeen(partitionBookmarks);
    }

    public static String lowestKeySeen(Map<String, String> bookmarkMap) {
        String lowest = null;
        for (String val: bookmarkMap.values()) {
            if (lowest == null || val.compareTo(lowest) < 0) {
                lowest = val;
            }
        }
        return lowest;
    }

    public RangeQueryResponse() {
        objectIds = new ArrayList<>();
        partitionBookmarks = new HashMap<>();
    }

    @SuppressWarnings("unchecked")
    public static Map<String, String> tokenToBookmarkMap(
        String token, LHConfig config
    ) {
        if (token == null || token.equals("null")) {
            return new HashMap<String, String>();

        } else {
            return Map.class.cast(LHUtil.stringToObj(
                new String(Base64.getDecoder().decode(token)),
                config
            ));
        }
    }

    public static String bookmarkMapToString(Map<String, String> bookmarkMap) {
        return Base64.getEncoder().encodeToString(
            LHUtil.objToString(bookmarkMap).getBytes()
        );
    }

    public RangeQueryResponse add(RangeQueryResponse other) {
        RangeQueryResponse out = new RangeQueryResponse();

        for (Map.Entry<String, String> e : partitionBookmarks.entrySet()) {
            out.partitionBookmarks.put(e.getKey(), e.getValue());
        }

        for (Map.Entry<String, String> e : other.partitionBookmarks.entrySet()) {
            out.partitionBookmarks.put(e.getKey(), e.getValue());
        }

        out.objectIds = new ArrayList<>();
        out.objectIds.addAll(objectIds);
        out.objectIds.addAll(other.objectIds);

        LHUtil.log("After add, ", objectIds.size(), other.objectIds.size(), out.objectIds.size());

        return out;
    }
}
