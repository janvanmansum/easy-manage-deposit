CREATE TABLE IF NOT EXISTS deposit_properties (
    uuid CHAR(36) NOT NULL,
    last_modified TIMESTAMPTZ NOT NULL,
    properties TEXT NOT NULL,
    state_label VARCHAR(30) NOT NULL,
    depositor_user_id VARCHAR(50) NOT NULL,
    datamanager VARCHAR(50),
    location VARCHAR(100) NOT NULL,
    has_bag BOOLEAN NOT NULL,
    storage_size_in_bytes bigint NOT NULL,
    number_of_continued_deposits INTEGER NOT NULL,
    PRIMARY KEY (uuid));
CREATE INDEX last_modified_idx ON deposit_properties(last_modified);
CREATE INDEX state_label_idx ON deposit_properties(state_label);
CREATE INDEX depositor_user_id_idx ON deposit_properties(depositor_user_id);
CREATE INDEX datamanager_idx ON deposit_properties(datamanager);

GRANT INSERT, SELECT, UPDATE, DELETE ON deposit_properties TO easy_manage_deposit;
