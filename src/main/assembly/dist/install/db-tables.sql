CREATE TABLE IF NOT EXISTS deposit_properties (
    uuid CHAR(36) NOT NULL,
    last_modified TIMESTAMP,
    properties TEXT NOT NULL,
    status_label VARCHAR(30),
    depositor_user_id VARCHAR(50),
    datamanager VARCHAR(50),
    PRIMARY KEY (uuid);

GRANT INSERT, SELECT, UPDATE, DELETE ON deposit_properties TO easy_manage_deposit;
