CREATE TABLE IF NOT EXISTS deposit_properties (
    uuid CHAR(36) NOT NULL,
    last_modified TIMESTAMP NOT NULL,
    properties TEXT NOT NULL,
    state_label VARCHAR(30) NOT NULL,
    depositor_user_id VARCHAR(50) NOT NULL,
    datamanager VARCHAR(50),
    storage_size_in_bytes bigint NOT NULL,
    location VARCHAR(100) NOT NULL,
    PRIMARY KEY (uuid));

GRANT INSERT, SELECT, UPDATE, DELETE ON deposit_properties TO easy_manage_deposit;
