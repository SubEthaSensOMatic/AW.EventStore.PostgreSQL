using Microsoft.Extensions.Logging;
using Npgsql;
using Polly;
using System;
using System.Threading;
using System.Threading.Tasks;

namespace AW.EventStore.PostgreSQL;

public class EventStoreInitializer
{
    private readonly ILogger<EventStoreInitializer> _logger;
    private readonly StoreConfiguration _configuration;
    private readonly IEventStoreDataSourceProvider _dataSourceProvider;

    public EventStoreInitializer(
        ILogger<EventStoreInitializer> logger,
        StoreConfiguration configuration,
        IEventStoreDataSourceProvider dataSourceProvider)
    {
        ArgumentNullException.ThrowIfNull(logger, nameof(logger));
        ArgumentNullException.ThrowIfNull(configuration, nameof(configuration));
        ArgumentNullException.ThrowIfNull(dataSourceProvider, nameof(dataSourceProvider));

        if (string.IsNullOrWhiteSpace(configuration.Schema))
            throw new InvalidOperationException("No database schema for postgresql event store defined.");

        _logger = logger;
        _configuration = configuration;
        _dataSourceProvider = dataSourceProvider;
    }

    public async Task Initialize(CancellationToken cancel)
    {
        var dataSource = await _dataSourceProvider.GetDataSource();

        await WaitForDatabase(dataSource, cancel);

        await CreateSchema(dataSource, _configuration.Schema);

        await CreateEventsTable(dataSource, _configuration.Schema);

        await CreateSnapshotTable(dataSource, _configuration.Schema);

        await CreateNewStreamProcedure(dataSource, _configuration.Schema);

        await CreateAppendToStreamProcedure(dataSource, _configuration.Schema);

        await CreateSnapshotProcedure(dataSource, _configuration.Schema);
    }

    private async Task WaitForDatabase(NpgsqlDataSource dataSource, CancellationToken cancel)
    {
        var policy = Policy
            .Handle<NpgsqlException>()
            .WaitAndRetryAsync(
                5,
                retryAttempt => TimeSpan.FromSeconds(Math.Pow(2, retryAttempt)),
                (exc, timespan, retryCount, context) =>
                {
                    _logger.LogWarning("Error while connecting to database: {Message}", exc.Message);
                });

        await policy.ExecuteAsync(
            async cancel => await dataSource.OpenConnectionAsync(cancel),
            cancel);
    }

    private async Task CreateSchema(NpgsqlDataSource dataSource, string schema)
    {
        if (string.IsNullOrWhiteSpace(schema))
            throw new InvalidOperationException("No event store schema specified.");

        _logger.LogInformation("Creating event store schema '{schema}' if not exists.", schema);

        await using var cmd = dataSource.CreateCommand(
            @$"CREATE SCHEMA IF NOT EXISTS {schema}");

        await cmd.ExecuteNonQueryAsync();
    }

    private async Task CreateEventsTable(NpgsqlDataSource dataSource, string schema)
    {
        _logger.LogInformation("Creating events table '{schema}.events' if not exists.", schema);

        await using var tableCmd = dataSource.CreateCommand(@$"
            CREATE TABLE IF NOT EXISTS {schema}.events (
                event_id BIGSERIAL NOT NULL,
                event_type CHARACTER VARYING(128) NOT NULL,
                event_payload BYTEA NOT NULL,
                event_created_at TIMESTAMP WITH TIME ZONE NOT NULL,
                event_created_by CHARACTER VARYING(512) NULL,
                stream_id CHARACTER VARYING(512) NOT NULL,
                stream_type CHARACTER VARYING(128) NOT NULL,
                stream_version INTEGER NOT NULL,
                correlation_id CHARACTER VARYING(128) NULL,
                causation_id CHARACTER VARYING(128) NULL,
                transaction_id XID8 NOT NULL,
                CONSTRAINT pk_events PRIMARY KEY (event_id))
            ");

        await tableCmd.ExecuteNonQueryAsync();

        await using var indexCmd = dataSource.CreateCommand(@$"
                CREATE UNIQUE INDEX IF NOT EXISTS ux_event_stream_version
                    ON {schema}.events USING btree(
                        stream_id ASC NULLS LAST,
                        stream_version ASC NULLS LAST)");

        await indexCmd.ExecuteNonQueryAsync();
    }

    private async Task CreateSnapshotTable(NpgsqlDataSource dataSource, string schema)
    {
        _logger.LogInformation("Creating snapshots table '{schema}.snapshots' if not exists.", schema);

        await using var tableCmd = dataSource.CreateCommand(@$"
            CREATE TABLE IF NOT EXISTS {schema}.snapshots (
                stream_id CHARACTER VARYING(512) NOT NULL,
                stream_version INTEGER NOT NULL,
                snapshot_type CHARACTER VARYING(128) NOT NULL,
                snapshot BYTEA NOT NULL,
                CONSTRAINT pk_snapshots PRIMARY KEY (stream_id))
            ");

        await tableCmd.ExecuteNonQueryAsync();
    }

    private async Task CreateNewStreamProcedure(NpgsqlDataSource dataSource, string schema)
    {
        _logger.LogInformation("Creating or replacing new stream procedure '{schema}.create_stream' if not exists.", schema);

        await using var cmd = dataSource.CreateCommand(@$"
            CREATE OR REPLACE PROCEDURE {schema}.create_stream(
	            v_event_created_by CHARACTER VARYING(512),
	            v_stream_id CHARACTER VARYING(512),
                v_stream_type CHARACTER VARYING(128),
                v_stream_version INTEGER[],
                v_event_type TEXT[],
                v_event_payload BYTEA[],      
                v_correlation_id CHARACTER VARYING(128),
                v_causation_id CHARACTER VARYING(128))
            LANGUAGE plpgsql
            AS $$
	            DECLARE v_created_at TIMESTAMP WITH TIME ZONE := now() at time zone 'utc';
	            DECLARE v_transcation_id XID8 := pg_current_xact_id();
            BEGIN
                INSERT INTO {schema}.events (
                    event_type, event_payload, event_created_at, event_created_by,
                    stream_id, stream_type, stream_version,
                    correlation_id, causation_id, transaction_id)
                SELECT e.event_type, e.event_payload, v_created_at, v_event_created_by,
                    v_stream_id, v_stream_type, e.stream_version, v_correlation_id, v_causation_id, v_transcation_id
                FROM UNNEST(v_stream_version, v_event_type, v_event_payload) AS e(stream_version, event_type, event_payload);

	            -- Alles committen
	            COMMIT;
            END $$; ");

        await cmd.ExecuteNonQueryAsync();
    }

    private async Task CreateAppendToStreamProcedure(NpgsqlDataSource dataSource, string schema)
    {
        _logger.LogInformation("Creating or replacing append to stream procedure '{schema}.append_to_stream' if not exists.", schema);

        await using var cmd = dataSource.CreateCommand(@$"
            CREATE OR REPLACE PROCEDURE {schema}.append_to_stream(
	            v_event_created_by CHARACTER VARYING(512),
	            v_stream_id CHARACTER VARYING(512),
                v_stream_version INTEGER[],
                v_event_type TEXT[],
                v_event_payload BYTEA[],      
                v_correlation_id CHARACTER VARYING(128),
                v_causation_id CHARACTER VARYING(128))
            LANGUAGE plpgsql
            AS $$
	            DECLARE v_created_at TIMESTAMP WITH TIME ZONE := now() at time zone 'utc';
                DECLARE v_stream_type CHARACTER VARYING(128);
                DECLARE v_stream_current_version INTEGER;
	            DECLARE v_transcation_id XID8 := pg_current_xact_id();
            BEGIN
                SELECT MIN(stream_type), MAX(stream_version)
                    INTO v_stream_type, v_stream_current_version
                FROM {schema}.events
                WHERE stream_id = v_stream_id;

                IF v_stream_type IS NULL OR v_stream_current_version IS NULL THEN
                    RAISE EXCEPTION SQLSTATE 'P0001' USING MESSAGE='Stream does not exist.';
                END IF;

                IF COALESCE(v_stream_version[1], -1) <> (v_stream_current_version + 1) THEN
                    RAISE EXCEPTION SQLSTATE 'P0002' USING MESSAGE='Stream already modified.';
                END IF;
	
                INSERT INTO {schema}.events (
                    event_type, event_payload, event_created_at, event_created_by,
                    stream_id, stream_type, stream_version,
                    correlation_id, causation_id, transaction_id)
                SELECT e.event_type, e.event_payload, v_created_at, v_event_created_by,
                    v_stream_id, v_stream_type, e.stream_version, v_correlation_id, v_causation_id, v_transcation_id
                FROM UNNEST(v_stream_version, v_event_type, v_event_payload) AS e(stream_version, event_type, event_payload);
	
	            -- Alles committen
	            COMMIT;
            END $$; ");

        await cmd.ExecuteNonQueryAsync();
    }

    private async Task CreateSnapshotProcedure(NpgsqlDataSource dataSource, string schema)
    {
        _logger.LogInformation("Creating or replacing procedure to create snapshots '{schema}.snapshot' if not exists.", schema);

        await using var cmd = dataSource.CreateCommand(@$"
            CREATE OR REPLACE PROCEDURE {schema}.snapshot(
	            v_stream_id CHARACTER VARYING(512),
                v_stream_version INTEGER,
                v_snapshot_type CHARACTER VARYING(128),
                v_snapshot BYTEA)
            LANGUAGE plpgsql
            AS $$
                DECLARE v_stream_current_version INTEGER;
            BEGIN
                SELECT MAX(stream_version) INTO v_stream_current_version
                FROM {schema}.events
                WHERE stream_id = v_stream_id;

                IF v_stream_current_version IS NULL THEN
                    RAISE EXCEPTION SQLSTATE 'P0001' USING MESSAGE='Stream does not exist.';
                END IF;

                IF v_stream_version > v_stream_current_version THEN
                    RAISE EXCEPTION SQLSTATE 'P0003' USING MESSAGE='Stream version does not exist.';
                END IF;

                INSERT INTO {schema}.snapshots (stream_id, snapshot_type, stream_version, snapshot)
                VALUES (v_stream_id, v_snapshot_type, v_stream_version, v_snapshot) 
                ON CONFLICT ON CONSTRAINT pk_snapshots 
                DO UPDATE SET stream_version = v_stream_version, snapshot = v_snapshot, snapshot_type = v_snapshot_type;
	        END $$; ");

        await cmd.ExecuteNonQueryAsync();
    }
}
