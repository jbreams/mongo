#include "mongo/db/namespace_string.h"
#include "mongo/db/pipeline/expression_context.h"
#include "mongo/platform/basic.h"

#include "mongo/db/commands.h"
#include "mongo/db/operation_context.h"
#include "mongo/db/pipeline/pipeline.h"
#include "mongo/db/pipeline/pipeline_d.h"
#include "mongo/db/service_context.h"
#include "mongo/migrator/oracle_helpers.h"
#include "mongo/migrator/document_source_oracle.h"
#include "mongo/migrator/agg_from_oracle_command_gen.h"
#include "mongo/util/intrusive_counter.h"

namespace mongo::migrator {
namespace {

const auto getOracleContext = ServiceContext::declareDecoration<std::unique_ptr<OracleContext>>();

} // namespace

class CmdAggFromOracle : public AggregateFromOracleCmdVersion2Gen<CmdAggFromOracle> {
public:
    std::string help() const final {
        return "Runs an aggregation pipeline against and Oracle database.";
    }

    AllowedOnSecondary secondaryAllowed(ServiceContext*) const final {
        return AllowedOnSecondary::kOptIn;
    }

    bool requiresAuth() const override {
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
            return NamespaceString(request().getDbName(), "");
        }

        Reply typedRun(OperationContext* opCtx) final;
    };
} cmdAggFromOracle;

CmdAggFromOracle::Reply CmdAggFromOracle::Invocation::typedRun(OperationContext *opCtx) {
    const auto& req = request();

    auto runtimeConstants = Variables::generateRuntimeConstants(opCtx);

    auto expCtx = make_intrusive<ExpressionContext>(
        opCtx,
        boost::none,
        false,  // fromMongos
        false,  // needsmerge
        false,  // allowDiskUse
        false,  // bypassDocumentValidation
        false,  // isMapReduceCommand
        NamespaceString::makeCollectionlessAggregateNSS(req.getDbName()),
        runtimeConstants,
        nullptr,
        MongoProcessInterface::create(opCtx),
        StringMap<ExpressionContext::ResolvedNamespace>{},  // resolvedNamespaces
        UUID::gen(),
        boost::none, // let
        false  // mayDbProfile
    );
    MakePipelineOptions pipelineOpts;
    pipelineOpts.attachCursorSource = false;
    pipelineOpts.allowTargetingShards = false;
    pipelineOpts.optimize = false;
    pipelineOpts.validator = [](const Pipeline&) {};

    auto pipeline = Pipeline::makePipeline(req.getPipeline(), expCtx, pipelineOpts);

    auto& oracleCtx = getOracleContext(opCtx->getServiceContext());
    if (!oracleCtx) {
        oracleCtx = OracleContext::make();
    }

    OracleConnectionOptions connectOpts;
    connectOpts.connString = req.getConnectionString();
    connectOpts.username = req.getUsername();
    connectOpts.password = req.getPassword();

    auto conn = OracleConnection::make(oracleCtx.get(), connectOpts);
    auto stmt = conn.prepareStatement(req.getQuery());
    stmt.execute();

    auto root_document_source = make_intrusive<DocumentSourceOracle>(stmt, expCtx);
    pipeline->addInitialSource(std::move(root_document_source));

    std::vector<BSONObj> batch;
    for (auto res = pipeline->getNext(); res; res = pipeline->getNext()) {
        batch.emplace_back(res->toBson());
    }

    return Reply{std::move(batch)};
}

} // namespace mongo::migrator
