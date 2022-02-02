package little.horse.common.util;

import java.util.regex.Pattern;

public class Constants {
    public static String KAFKA_BOOTSTRAP_SERVERS_KEY = "LHORSE_KAFKA_BOOTSTRAP_SERVERS";
    public static String KAFKA_TOPIC_PREFIX_KEY = "LHORSE_KAFKA_TOPIC_PREFIX";
    public static String KAFKA_APPLICATION_ID_KEY = "LHORSE_KAFKA_APPLICATION_ID";

    public static String DEFAULT_PARTITIONS_KEY = "LHORSE_DEFAULT_PARTITIONS";

    public static String SYSTEM_PREFIX = "SYSTEM__";

    public static String API_URL_KEY = "LHORSE_API_URL";
    public static String ADVERTISED_URL_KEY = "LHORSE_ADVERTISED_URL";
    public static String STATE_DIR_KEY = "LHORSE_KAFKA_STREAMS_STATE_DIR";

    public static String WF_SPEC_GUID_KEY = "LHORSE_WF_SPEC_GUID";
    public static String NODE_NAME_KEY = "LHORSE_NODE_NAME";
    public static String THREAD_SPEC_NAME_KEY = "LHORSE_THREAD_SPEC_NAME";

    public static String DEFAULT_TASK_IMAGE_KEY = "LHORSE_DEFAULT_TASK_IMAGE";
    public static String DEFAULT_COLLECTOR_IMAGE_KEY = "LHORSE_DEFAULT_COLLECTOR_IMAGE";
    public static String DEFAULT_REPLICAS_KEY = "LHORSE_DEFAULT_K8S_REPLICAS";

    public static String WF_SPEC_API_PATH = "wfSpec";
    public static String TASK_DEF_API_PATH = "taskDef";
    public static String EXTERNAL_EVENT_DEF_PATH = "externalEventDef";
    public static String WF_RUN_API_PATH = "wfRun";

    // Should only be used by the actual API. TODO: Move this elsewhere.
    public static String TASK_DEF_GUID_STORE = "taskDefGuidStore";
    public static String TASK_DEF_NAME_STORE = "taskDefNameStore";
    public static String WF_SPEC_GUID_STORE = "wfSpecGuidStore";
    public static String WF_SPEC_NAME_STORE = "wfSpecNameStore";
    public static String EXTERNAL_EVENT_DEF_NAME_STORE = "externalEventDefStore";
    public static String EXTERNAL_EVENT_DEF_GUID_STORE = "externalEventDefGuidStore";
    public static String WF_RUN_STORE = "wfRunStorePlus";

    public static int EXPOSED_PORT = 5000;

    public static Pattern VARIABLE_PATTERN = Pattern.compile(
        "<<(.*?)>>"
    );
}
