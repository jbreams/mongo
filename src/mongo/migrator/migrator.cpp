#include "mongo/logv2/log_domain_global.h"
#include "mongo/logv2/log_format.h"
#include "mongo/logv2/log_manager.h"
#include "mongo/platform/basic.h"

#include "mongo/base/initializer.h"
#include "mongo/db/service_context.h"
#include "mongo/migrator/oracle_helpers.h"
#include "mongo/util/signal_handlers.h"

namespace mongo::migrator {

int main(int argc, char** argv) {
    mongo::setupSignalHandlers();

    {
        auto& lv2Manager = logv2::LogManager::global();
        logv2::LogDomainGlobal::ConfigurationOptions lv2Config;
        uassertStatusOK(lv2Manager.getGlobalDomainInternal().configure(lv2Config));
    }

    mongo::runGlobalInitializersOrDie(std::vector<std::string>(argv, argv + argc));
    setGlobalServiceContext(ServiceContext::make());

    auto oracleCtx = OracleContext::make();
    OracleConnectionPool::CreateOptions opts;
    opts.connString = "";
    opts.username = "";
    opts.password = "";
    auto connPool = OracleConnectionPool::make(oracleCtx.get(), std::move(opts));
    auto conn = connPool.acquireConnection();

    auto statement = conn.prepareStatement("select * from v$version");
    statement.execute();

    return 0;
}

} // namespace mongo::migrator
int main(int argc, char** argv) {
    return mongo::migrator::main(argc, argv);
}
