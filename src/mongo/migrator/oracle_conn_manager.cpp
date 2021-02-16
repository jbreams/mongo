#include "mongo/db/operation_context.h"
#include "mongo/db/service_context.h"
#include "mongo/idl/basic_types_gen.h"
#include "mongo/migrator/document_source_oracle_gen.h"
#include "mongo/migrator/oracle_helpers.h"
#include "mongo/platform/basic.h"

#include "mongo/migrator/oracle_conn_manager.h"
#include <memory>

namespace mongo::migrator {
namespace {

const auto getFromSvcCtx =
    ServiceContext::declareDecoration<std::unique_ptr<OracleConnectionManager>>();

} // namespace

OracleConnectionManager* OracleConnectionManager::get(ServiceContext *svcCtx) {
    return getFromSvcCtx(svcCtx).get();
}

OracleConnectionManager* OracleConnectionManager::configure(
        ServiceContext *svcCtx, const OracleConnectionOptions& opts) {
    auto& holder = getFromSvcCtx(svcCtx);
    fassert(0, !holder);

    holder = std::make_unique<OracleConnectionManager>(opts);
    return holder.get();
}

OracleConnectionManager::OracleConnectionManager(const OracleConnectionOptions& opts)
    : _context(),
      _pool(&_context, opts)
{}

OracleConnection OracleConnectionManager::acquireConnection() {
    return _pool.acquireConnection();
}

class CmdConfigureOracle : public ConfigureOracleCmdVersion1Gen<CmdConfigureOracle> {
public:
    std::string help() const final {
        return "Configures the connection parameter to be used by the $oracle agg stage";
    }

    AllowedOnSecondary secondaryAllowed(ServiceContext*) const final {
        return AllowedOnSecondary::kOptIn;
    }

    bool requiresAuth() const final {
        return true;
    }

    class Invocation final : public InvocationBaseGen {
    public:
        using InvocationBaseGen::InvocationBaseGen;

        bool supportsWriteConcern() const final {
            return false;
        }

        void doCheckAuthorization(OperationContext*) const final {}
        
        NamespaceString ns() const final {
            return NamespaceString(request().getDbName(), "$cmd");
        }

        Reply typedRun(OperationContext* opCtx) final;
    };
} cmdConfigureOracle;

CmdConfigureOracle::Reply CmdConfigureOracle::Invocation::typedRun(OperationContext* opCtx) try {
    auto svcCtx = opCtx->getServiceContext();
    uassert(0, "Oracle connection already configured",
            OracleConnectionManager::get(svcCtx) == nullptr);

    OracleConnectionOptions opts;
    opts.connString = request().getConnectionString();
    opts.password = request().getPassword();
    opts.username = request().getUsername();

    auto connMgr = OracleConnectionManager::configure(svcCtx, opts);

    auto conn = connMgr->acquireConnection();
    auto versionStmt = conn.prepareStatement("select banner from v$version");
    versionStmt.execute();
    uassert(0, "Error getting version banner from Oracle connection", versionStmt.fetch());

    Reply reply;
    reply.setOracleVersion(versionStmt.getColumnValue(1).as<std::string_view>());

    return reply;
} catch(const OracleException& e) {
    uasserted(ErrorCodes::InternalError, e.what());
}

} // namespace mongo::migrator
