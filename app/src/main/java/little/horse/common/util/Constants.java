package little.horse.common.util;

import java.time.Duration;
import java.util.regex.Pattern;

public class Constants {
    public static String KAFKA_BOOTSTRAP_SERVERS_KEY = "LHORSE_KAFKA_BOOTSTRAP_SERVERS";
    public static String KAFKA_TOPIC_PREFIX_KEY = "LHORSE_KAFKA_TOPIC_PREFIX";
    public static String KAFKA_APPLICATION_ID_KEY = "LHORSE_KAFKA_APPLICATION_ID";
    public static String KAFKA_APPLICATION_IID_KEY = "LHORSE_KAFKA_APPLICATION_IID";

    public static String DEFAULT_PARTITIONS_KEY = "LHORSE_DEFAULT_PARTITIONS";

    public static String SYSTEM_PREFIX = "SYSTEM__";

    public static String API_URL_KEY = "LHORSE_API_URL";
    public static String ADVERTISED_PROTOCOL_KEY = "LHORSE_ADVERTISED_PROTOCOL";
    public static String ADVERTISED_HOST_KEY = "LHORSE_ADVERTISED_HOST";
    public static String ADVERTISED_PORT_KEY = "LHORSE_ADVERTISED_PORT";
    public static String STATE_DIR_KEY = "LHORSE_KAFKA_STREAMS_STATE_DIR";

    public static String DEFAULT_WF_WORKER_IMAGE_KEY = "LHORSE_DEFAULT_WF_WORKER_IMAGE";
    public static String DEFAULT_REPLICAS_KEY = "LHORSE_DEFAULT_REPLICAS";
    public static String DEFAULT_TASK_DEPLOYER_KEY = "LHORSE_DEFAULT_TASK_DEPLOYER";
    public static String DEFAULT_WF_DEPLOYER_KEY = "LHORSE_DEFAULT_WF_DEPLOYER";

    public static String DEFAULT_TASK_WORKER_POLL_MILLIS_KEY =
        "LHORSE_DEFAULT_TASK_WORKER_POLL_MILLIS";

    // TODO: make this a reserved word.
    public static String LATEST_OFFSET_ROCKSDB_KEY = "latest-offset";
    public static String LATEST_TS_ROCKSDB_KEY = "latest-timestamp";
    public static String LAST_PROCESSED_TS_ROCKSDB_KEY = "last-processed-timestamp";
    public static String TIMER_STORE_NAME = "timerStore";
    public static Duration PUNCTUATOR_INERVAL = Duration.ofSeconds(1);

    public static int EXPOSED_PORT = 5000;

    public static Pattern VARIABLE_PATTERN = Pattern.compile(
        "<<(.*?)>>"
    );
    public static String WF_RUN_STORE_NAME = "wfRunStore";

    public static String FORCE_LOCAL = "forceLocal";
}
