CREATE TYPE IF NOT EXISTS lh_deploy_status AS ENUM (
    'STARTING',
    'RUNNING',
    'COMPLETED',
    'STOPPING',
    'STOPPED',
    'DESIRED_REDEPLOY',
    'ERROR'
);


CREATE TYPE IF NOT EXISTS wf_run_variable_type AS ENUM (
    'OBJECT',
    'ARRAY',
    'INT',
    'FLOAT',
    'BOOLEAN',
    'STRING'
);


CREATE TABLE IF NOT EXISTS wf_run_variable_def (
    type wf_run_variable_type,
    
)