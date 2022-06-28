package io.littlehorse.common.util;

import java.time.Duration;

public class Constants {
    public static String KAFKA_BOOTSTRAP_SERVERS_KEY = "LHORSE_KAFKA_BOOTSTRAP_SERVERS";
    public static String KAFKA_TOPIC_PREFIX_KEY = "LHORSE_KAFKA_TOPIC_PREFIX";
    public static String KAFKA_APPLICATION_ID_KEY = "LHORSE_KAFKA_APPLICATION_ID";
    public static String KAFKA_APPLICATION_IID_KEY = "LHORSE_KAFKA_APPLICATION_IID";

    public static String DEFAULT_PARTITIONS_KEY = "LHORSE_DEFAULT_PARTITIONS";
    public static String SYSTEM_PREFIX = "SYSTEM__";

    public static String API_URL_KEY = "LHORSE_API_URL";
    public static String STATE_DIR_KEY = "LHORSE_KAFKA_STREAMS_STATE_DIR";
    public static String EXPOSE_KSTREAMS_HEALTH_KEY = "LHORSE_EXPOSE_KSTREAMS_HEALTH";
    public static int EXPOSED_PORT = 5000;

    public static String DEFAULT_REPLICAS_KEY = "LHORSE_DEFAULT_REPLICAS";
    public static String DEFAULT_TASK_WORKER_POLL_MILLIS_KEY =
        "LHORSE_DEFAULT_TASK_WORKER_POLL_MILLIS";

    public static String TIMER_STORE_NAME = "timerStore";
    public static String WF_RUN_STORE_NAME = "wfRunStore";
    public static Duration PUNCTUATOR_INERVAL = Duration.ofSeconds(1);

    public static String WF_RUN_ENTITY_TOPIC = "WFRun_Entity";
    public static String WF_RUN_EVENT_TOPIC = "WFRun_Event";
    public static String TASK_RUN_ENTITY_TOPIC = "TaskRun_Entity";
}
