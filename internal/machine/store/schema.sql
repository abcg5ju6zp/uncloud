-- cluster table stores the key-value pairs of the cluster configuration.
CREATE TABLE cluster
(
    key        TEXT      NOT NULL PRIMARY KEY,
    value      ANY,
    -- updated_at is the last time the value was written.
    updated_at TIMESTAMP NOT NULL DEFAULT '1970-01-01 00:00:00'
);

-- machines table stores the basic information of the machines in the cluster.
CREATE TABLE machines
(
    id         TEXT      NOT NULL PRIMARY KEY,
    name       TEXT AS (json_extract(info, '$.name')),
    -- info is a JSON-serialized MachineInfo protobuf message.
    info       TEXT      NOT NULL DEFAULT '{}' CHECK (json_valid(info)),
    -- created_at is the time the machine record was created.
    created_at TIMESTAMP NOT NULL DEFAULT '1970-01-01 00:00:00',
    -- updated_at is the last time the machine record was updated.
    updated_at TIMESTAMP NOT NULL DEFAULT '1970-01-01 00:00:00'
);

-- containers table stores the Uncloud-managed Docker containers created in the cluster.
CREATE TABLE containers
(
    id           TEXT      NOT NULL PRIMARY KEY,
    -- container is a JSON-serialized api.ServiceContainer struct.
    container    TEXT      NOT NULL DEFAULT '{}' CHECK (json_valid(container)),
    machine_id   TEXT      NOT NULL DEFAULT '',
    service_id   TEXT AS (json_extract(container, '$.Config.Labels."uncloud.service.id"')),
    service_name TEXT AS (json_extract(container, '$.Config.Labels."uncloud.service.name"')),
    -- sync_status indicates if the record reflects the actual Docker state of the container.
    sync_status  TEXT      NOT NULL DEFAULT '',
    -- updated_at is the last time the record was updated.
    updated_at   TIMESTAMP NOT NULL DEFAULT '1970-01-01 00:00:00'
);

-- container_generations records how much each machine trusts its own rows in the containers table.
-- A generation is trusted only after the machine establishes a Docker event subscription, takes a
-- complete snapshot of its managed containers (regular service containers and deployment hooks),
-- and applies all deletions and upserts to the store, including events that arrived in between.
-- Consumers (DNS, Caddy, service queries, deployments) must only use containers of trusted, fresh
-- generations. Each machine writes only its own row identified by its machine ID.
CREATE TABLE container_generations
(
    machine_id  TEXT      NOT NULL PRIMARY KEY,
    -- generation is a monotonically increasing number, bumped on every resubscription to Docker
    -- events (uncloudd restart, Docker daemon restart, broken event stream, failed sync).
    generation  INTEGER   NOT NULL DEFAULT 0,
    -- trusted is 1 when the snapshot of this generation and all events up to it are reflected
    -- in the containers table. 0 means the machine's container facts are uncertain.
    trusted     INTEGER   NOT NULL DEFAULT 0,
    -- reason explains why the generation is not trusted. See GenerationReason* constants.
    reason      TEXT      NOT NULL DEFAULT '',
    -- updated_at is the last time the generation was published. Trusted rows are refreshed
    -- periodically as a heartbeat, so a stale trusted row can be treated as untrusted.
    updated_at  TIMESTAMP NOT NULL DEFAULT '1970-01-01 00:00:00'
);

CREATE INDEX idx_machines_name ON machines (name);

CREATE INDEX idx_containers_machine_id ON containers (machine_id);
CREATE INDEX idx_containers_service_id ON containers (service_id);
CREATE INDEX idx_containers_service_name ON containers (service_name);
