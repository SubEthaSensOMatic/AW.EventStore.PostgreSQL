using Npgsql;
using NpgsqlTypes;
using System.Collections.Generic;
using System;
using System.Text.Json;
using System.Threading.Tasks;
using System.Threading;
using System.Linq;
using AW.NamedTypes;
using AW.EventStore.PostrgreSQL;

namespace AW.EventStore.PostgreSQL;

public class EventStore : IEventStore
{
    private readonly IEventStoreDataSourceProvider _dataSourceProvider;
    private readonly StoreConfiguration _configuration;
    private readonly IEventStoreNotifications _eventStoreNotifications;

    public EventStore(
        IEventStoreDataSourceProvider dataSourceProvider,
        StoreConfiguration configuration,
        IEventStoreNotifications eventStoreNotifications)
    {
        ArgumentNullException.ThrowIfNull(configuration, nameof(configuration));
        ArgumentNullException.ThrowIfNull(dataSourceProvider, nameof(dataSourceProvider));
        ArgumentNullException.ThrowIfNull(eventStoreNotifications, nameof(eventStoreNotifications));

        if (string.IsNullOrWhiteSpace(configuration.Schema))
            throw new InvalidOperationException("No database schema for postgresql event store defined.");

        _dataSourceProvider = dataSourceProvider;
        _configuration = configuration;
        _eventStoreNotifications = eventStoreNotifications;
    }

    public async Task CreateStream(
        string streamId, string streamType, IEnumerable<object> eventPayloads,
        string? user = null, string? correlationId = null, string? causationId = null)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(streamId, nameof(streamId));
        ArgumentException.ThrowIfNullOrWhiteSpace(streamType, nameof(streamType));
        ArgumentNullException.ThrowIfNull(eventPayloads, nameof(eventPayloads));

        var parameterPayloads = new string[eventPayloads.Count()];
        var parameterEventTypes = new string[parameterPayloads.Length];
        var parameterVersions = new int[parameterPayloads.Length];

        var payloadEnumerator = eventPayloads.GetEnumerator();
        for (var i = 0; i < parameterPayloads.Length; i++)
        {
            payloadEnumerator.MoveNext();
            var eventPayload = payloadEnumerator.Current;

            if (TypeRegistry.Instance.TryResolveName(eventPayload.GetType(), out var eventName) == false || eventName == null)
                throw new InvalidOperationException($"Event type '{eventPayload.GetType()}' is not mapped.");

            parameterPayloads[i] = JsonSerializer.Serialize(eventPayload);
            parameterEventTypes[i] = eventName;
            parameterVersions[i] = i + 1;
        }

        var dataSource = await _dataSourceProvider.GetDataSource();

        await using var cmd = dataSource.CreateCommand(
            @$"CALL {_configuration.Schema}.create_stream($1, $2, $3, $4, $5, $6, $7, $8)");

        cmd.Parameters.AddWithValue(NpgsqlDbType.Text, user ?? (object)DBNull.Value);
        cmd.Parameters.AddWithValue(NpgsqlDbType.Text, streamId);
        cmd.Parameters.AddWithValue(NpgsqlDbType.Text, streamType);
        cmd.Parameters.AddWithValue(NpgsqlDbType.Integer | NpgsqlDbType.Array, parameterVersions);
        cmd.Parameters.AddWithValue(NpgsqlDbType.Text | NpgsqlDbType.Array, parameterEventTypes);
        cmd.Parameters.AddWithValue(NpgsqlDbType.Text | NpgsqlDbType.Array, parameterPayloads);
        cmd.Parameters.AddWithValue(NpgsqlDbType.Text, correlationId ?? (object)DBNull.Value);
        cmd.Parameters.AddWithValue(NpgsqlDbType.Text, causationId ?? (object)DBNull.Value);

        await cmd.ExecuteNonQueryAsync();

        _eventStoreNotifications.Publish(new StreamChangedNotification(streamId, parameterVersions.Last()));
    }

    public async Task AppendToStream(
        string streamId, int expectedStreamVersion, IEnumerable<object> eventPayloads,
        string? user = null, string? correlationId = null, string? causationId = null)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(streamId, nameof(streamId));
        ArgumentOutOfRangeException.ThrowIfLessThan(expectedStreamVersion, 1, nameof(expectedStreamVersion));
        ArgumentNullException.ThrowIfNull(eventPayloads, nameof(eventPayloads));

        var parameterPayloads = new string[eventPayloads.Count()];
        var parameterEventTypes = new string[parameterPayloads.Length];
        var parameterVersions = new int[parameterPayloads.Length];

        var payloadEnumerator = eventPayloads.GetEnumerator();
        for (var i = 0; i < parameterPayloads.Length; i++)
        {
            payloadEnumerator.MoveNext();
            var eventPayload = payloadEnumerator.Current;

            if (TypeRegistry.Instance.TryResolveName(eventPayload.GetType(), out var eventName) == false || eventName == null)
                throw new InvalidOperationException($"Event type '{eventPayload.GetType()}' is not mapped.");

            parameterPayloads[i] = JsonSerializer.Serialize(eventPayload);
            parameterEventTypes[i] = eventName;
            parameterVersions[i] = i + expectedStreamVersion + 1;
        }

        var dataSource = await _dataSourceProvider.GetDataSource();

        await using var cmd = dataSource.CreateCommand(
            @$"CALL {_configuration.Schema}.append_to_stream($1, $2, $3, $4, $5, $6, $7)");

        cmd.Parameters.AddWithValue(NpgsqlDbType.Text, user ?? (object)DBNull.Value);
        cmd.Parameters.AddWithValue(NpgsqlDbType.Text, streamId);
        cmd.Parameters.AddWithValue(NpgsqlDbType.Integer | NpgsqlDbType.Array, parameterVersions);
        cmd.Parameters.AddWithValue(NpgsqlDbType.Text | NpgsqlDbType.Array, parameterEventTypes);
        cmd.Parameters.AddWithValue(NpgsqlDbType.Text | NpgsqlDbType.Array, parameterPayloads);
        cmd.Parameters.AddWithValue(NpgsqlDbType.Text, correlationId ?? (object)DBNull.Value);
        cmd.Parameters.AddWithValue(NpgsqlDbType.Text, causationId ?? (object)DBNull.Value);

        await cmd.ExecuteNonQueryAsync();

        _eventStoreNotifications.Publish(
            new StreamChangedNotification(streamId, parameterVersions.Last()));
    }

    public async Task<IEnumerable<Event>> LoadStream(string streamId, CancellationToken cancel, int startVersion = 0)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(streamId, nameof(streamId));
        ArgumentNullException.ThrowIfNull(cancel, nameof(cancel));
        ArgumentOutOfRangeException.ThrowIfNegative(startVersion, nameof(startVersion));

        var dataSource = await _dataSourceProvider.GetDataSource();

        await using var cmd = dataSource.CreateCommand($@"
            SELECT event_type, event_id, event_payload, event_created_at, event_created_by, 
	            stream_id, stream_type, stream_version, correlation_id, causation_id, transaction_id
            FROM {_configuration.Schema}.events
            WHERE stream_id = $1 AND stream_version >= $2
                AND transaction_id < pg_snapshot_xmin(pg_current_snapshot())
            ORDER BY stream_version");

        cmd.Parameters.AddWithValue(NpgsqlDbType.Text, streamId);
        cmd.Parameters.AddWithValue(NpgsqlDbType.Integer, startVersion);

        await using var reader = await cmd.ExecuteReaderAsync(cancel);

        var result = new LinkedList<Event>();

        while (await reader.ReadAsync(cancel))
        {
            if (cancel.IsCancellationRequested)
                break;

            var eventName = reader.GetString(0);

            if (TypeRegistry.Instance.TryResolveType(eventName, out var eventType) == false || eventType == null)
                throw new InvalidOperationException($"Event '{eventName}' is not mapped.");

            var eventId = reader.GetInt64(1);
            var payloadJson = reader.GetString(2);
            var payload = JsonSerializer.Deserialize(payloadJson, eventType);

            if (payload == null)
                throw new InvalidOperationException($"Payload of event '{eventId}' is null.");

            var createdAt = reader.GetDateTime(3);
            var createdBy = reader.GetString(4);
            var streamid = reader.GetString(5);
            var streamType = reader.GetString(6);
            var streamVersion = reader.GetInt32(7);
            var correlationId = reader.IsDBNull(8) ? null : reader.GetString(8);
            var causationId = reader.IsDBNull(8) ? null : reader.GetString(9);
            var transactionId = (ulong)reader.GetValue(10);

            result.AddLast(new Event(
                new EventId(transactionId, eventId), eventName, createdAt, createdBy, payload,
                streamid, streamType, streamVersion, correlationId, causationId));
        }

        return result;
    }

    public async Task<IEnumerable<Event>> StreamEvents(IEventId? lastEventId, long maxCount, CancellationToken cancel)
    {
        ArgumentNullException.ThrowIfNull(cancel, nameof(cancel));
        ArgumentOutOfRangeException.ThrowIfLessThan(maxCount, 1, nameof(maxCount));

        var internalId = lastEventId as EventId;

        if (lastEventId != null && internalId == null)
            throw new InvalidOperationException("Invalid event id.");

        var sql = @$"
            SELECT event_type, event_id, event_payload, event_created_at, event_created_by, 
	            stream_id, stream_type, stream_version, correlation_id, causation_id, transaction_id
            FROM {_configuration.Schema}.events 
            WHERE (transaction_id, event_id) >= ($2, $1 + 1)
                AND transaction_id < pg_snapshot_xmin(pg_current_snapshot())
            ORDER BY transaction_id ASC, event_id ASC
            LIMIT {maxCount}";

        var dataSource = await _dataSourceProvider.GetDataSource();

        await using var cmd = dataSource.CreateCommand(sql);
        cmd.Parameters.AddWithValue(NpgsqlDbType.Bigint, (internalId?.Sequence) ?? 0L);
        cmd.Parameters.AddWithValue(NpgsqlDbType.Xid8, internalId?.TxId ?? 0ul);

        await using var reader = await cmd.ExecuteReaderAsync(cancel);

        var result = new LinkedList<Event>();

        while (await reader.ReadAsync(cancel))
        {
            if (cancel.IsCancellationRequested)
                break;

            var eventName = reader.GetString(0);

            if (TypeRegistry.Instance.TryResolveType(eventName, out var eventType) == false || eventType == null)
                throw new InvalidOperationException($"Event '{eventName}' is not mapped.");

            var eventId = (long)reader.GetValue(1);
            var payloadJson = reader.GetString(2);

            var payload = JsonSerializer.Deserialize(payloadJson, eventType);
            if (payload == null)
                throw new InvalidOperationException($"Payload of event '{eventId}' is null.");

            var createdAt = reader.GetDateTime(3);
            var createdBy = reader.GetString(4);
            var streamid = reader.GetString(5);
            var streamType = reader.GetString(6);
            var streamVersion = reader.GetInt32(7);
            var correlationId = reader.IsDBNull(8) ? null : reader.GetString(8);
            var causationId = reader.IsDBNull(9) ? null : reader.GetString(9);
            var transactionId = (ulong)reader.GetValue(10);

            result.AddLast(new Event(
                new EventId(transactionId, eventId), eventName, createdAt, createdBy, payload,
                streamid, streamType, streamVersion, correlationId, causationId));
        }

        return result;
    }

    public async Task CreateSnapshot(string streamId, int streamVersion, object state)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(streamId, nameof(streamId));
        ArgumentOutOfRangeException.ThrowIfLessThan(streamVersion, 1, nameof(streamVersion));
        ArgumentNullException.ThrowIfNull(state, nameof(state));

        if (TypeRegistry.Instance.TryResolveName(state.GetType(), out var snapshotName) == false || snapshotName == null)
            throw new InvalidOperationException($"Snapshot type '{state.GetType()}' is not mapped.");

        var snapshot = JsonSerializer.Serialize(state);

        var dataSource = await _dataSourceProvider.GetDataSource();

        await using var cmd = dataSource.CreateCommand(
            @$"CALL {_configuration.Schema}.snapshot($1, $2, $3, $4)");

        cmd.Parameters.AddWithValue(NpgsqlDbType.Text, streamId);
        cmd.Parameters.AddWithValue(NpgsqlDbType.Integer, streamVersion);
        cmd.Parameters.AddWithValue(NpgsqlDbType.Text, snapshotName);
        cmd.Parameters.AddWithValue(NpgsqlDbType.Text, snapshot);

        await cmd.ExecuteNonQueryAsync();
    }

    public async Task<Snapshot?> LoadSnapshot(string streamId)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(streamId, nameof(streamId));

        var dataSource = await _dataSourceProvider.GetDataSource();

        await using var cmd = dataSource.CreateCommand($@"
            SELECT snapshot_type, stream_version, snapshot
            FROM {_configuration.Schema}.snapshots
            WHERE stream_id = $1");

        cmd.Parameters.AddWithValue(NpgsqlDbType.Text, streamId);

        await using var reader = await cmd.ExecuteReaderAsync();

        object? snapshot = null;
        var version = 0;

        if (await reader.ReadAsync())
        {
            var snapshotName = reader.GetString(0);

            if (TypeRegistry.Instance.TryResolveType(snapshotName, out var snapshotType) == false || snapshotType == null)
                throw new InvalidOperationException($"Snapshot '{snapshotName}' is not mapped.");

            version = reader.GetInt32(1);
            var snapshotJson = reader.GetString(2);
            snapshot = JsonSerializer.Deserialize(snapshotJson, snapshotType);
        }

        return snapshot == null
            ? null
            : new(snapshot, version);
    }
}
