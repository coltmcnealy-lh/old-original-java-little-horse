CREATE TABLE wf_specs (
    id VARCHAR(256) PRIMARY KEY,
    created TIMESTAMP NOT NULL DEFAULT NOW(),
    updated TIMESTAMP,
    name VARCHAR(256),
    -- entrypoint_thread_name VARCHAR(256),

    spec JSON
);

-- CREATE TABLE thread_specs (
--     id VARCHAR(256) PRIMARY KEY,
--     created TIMESTAMP NOT NULL DEFAULT NOW(),
--     updated TIMESTAMP,
--     name VARCHAR(256),
--     wf_spec_id VARCHAR(256) REFERENCES wf_specs(id),
--     entrypoint_node_name VARCHAR(256),

--     spec JSON
-- );

-- CREATE TYPE node_type_enum AS ENUM (
--     'TASK',
--     'EXTERNAL_EVENT',
--     'SPAWN_THREAD',
--     'WAIT_FOR_THREAD',
--     'SLEEP',
--     'NOP',
--     'THROW_EXCEPTION'
-- );

-- CREATE TYPE lh_var_type_enum AS ENUM (
--     'INT',
--     'OBJECT',
--     'ARRAY',
--     'BOOLEAN',
--     'DOUBLE',
--     'STRING'
-- );

CREATE TABLE external_event_defs (
    id VARCHAR(256) PRIMARY KEY,
    created TIMESTAMP NOT NULL DEFAULT NOW(),
    updated TIMESTAMP,
    name VARCHAR(256),
    -- content_type lh_var_type_enum,

    spec JSON
);

-- CREATE TABLE nodes (
--     id VARCHAR(256) PRIMARY KEY,
--     created TIMESTAMP NOT NULL DEFAULT NOW(),
--     updated TIMESTAMP,
--     name VARCHAR(256),

--     num_retries INTEGER,
--     node_type node_type_enum,
--     variables JSON,
--     external_event_def_name VARCHAR(256) DEFAULT NULL,
--     external_event_def_id VARCHAR(256) DEFAULT NULL REFERENCES external_event_defs(id),

--     thread_wait_thread_id JSON,
    
-- );

CREATE TABLE task_defs (
    id VARCHAR(256) PRIMARY KEY,
    created TIMESTAMP NOT NULL DEFAULT NOW(),
    updated TIMESTAMP,
    name VARCHAR(256),

    spec JSON
);

CREATE TABLE wf_runs (
    id VARCHAR(512) PRIMARY KEY,
    created TIMESTAMP NOT NULL DEFAULT NOW(),
    updated TIMESTAMP,

    spec JSON
);