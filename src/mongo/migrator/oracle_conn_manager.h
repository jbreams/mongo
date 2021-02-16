#pragma once

#include "mongo/db/service_context.h"
#include "mongo/migrator/oracle_helpers.h"
namespace mongo::migrator {

class OracleConnectionManager {
public:
    explicit OracleConnectionManager(const OracleConnectionOptions& opts);

    OracleConnectionManager(const OracleConnectionManager&) = delete;
    OracleConnectionManager(OracleConnectionManager&&) = delete;
    OracleConnectionManager& operator=(const OracleConnectionManager&) = delete;
    OracleConnectionManager& operator=(OracleConnectionManager&&) = delete;

    static OracleConnectionManager* get(ServiceContext* svcCtx);
    static OracleConnectionManager* configure(
            ServiceContext* svcCtx, const OracleConnectionOptions& opts);

    OracleConnection acquireConnection();

private:
    OracleContext _context;
    OracleConnectionPool _pool;
};

} // namespace mongo::migrator
