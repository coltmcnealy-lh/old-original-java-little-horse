CREATE TABLE wf_specs (
    id VARCHAR(256) PRIMARY KEY,
    created TIMESTAMP NOT NULL DEFAULT NOW(),
    updated TIMESTAMP,
    name VARCHAR(256),
    entrypoint_thread_name VARCHAR(256)
);

CREATE TABLE thread_specs (
    id VARCHAR(256) PRIMARY KEY,
    created TIMESTAMP NOT NULL DEFAULT NOW(),
    updated TIMESTAMP,
    name VARCHAR(256),
    wf_spec_id VARCHAR(256) REFERENCES wf_specs(id),
    entrypoint_node_name VARCHAR(256)
);

CREATE TYPE node_type_enum AS ENUM (
    'TASK',
    'EXTERNAL_EVENT',
    'SPAWN_THREAD',
    'WAIT_FOR_THREAD',
    'SLEEP',
    'NOP',
    'THROW_EXCEPTION'
);

CREATE TABLE external_event_defs (

)

CREATE TABLE nodes (
    id VARCHAR(256) PRIMARY KEY,
    created TIMESTAMP NOT NULL DEFAULT NOW(),
    updated TIMESTAMP,
    name VARCHAR(256),

    num_retries INTEGER,
    node_type node_type_enum,
    variables JSON,
    external_event_def_name VARCHAR(256) DEFAULT NULL,
    external_event_def_id VARCHAR(256) DEFAULT NULL REFERENCES external_event_defs(id),

    thread_wait_thread_id JSON,
    
);