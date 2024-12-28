using Npgsql;
using System.Threading.Tasks;

namespace AW.EventStore.PostgreSQL;

public interface IEventStoreDataSourceProvider
{
    Task<NpgsqlDataSource> GetDataSource();
}
