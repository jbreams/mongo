#include "boost/smart_ptr/intrusive_ptr.hpp"
#include "mongo/bson/bsonelement.h"
#include "mongo/db/exec/document_value/document.h"
#include "mongo/db/namespace_string.h"
#include "mongo/db/operation_context.h"
#include "mongo/db/pipeline/document_source.h"
#include "mongo/db/pipeline/expression.h"
#include "mongo/db/pipeline/expression_context.h"
#include "mongo/migrator/document_source_oracle_gen.h"

namespace mongo::migrator {
class DocumentSourceOracle : public DocumentSource {
public:
    static constexpr StringData kStageName = "$oracle"_sd;

    static boost::intrusive_ptr<DocumentSource> createFromBson(
        BSONElement elem, const boost::intrusive_ptr<ExpressionContext>& expCtx);

    GetNextResult doGetNext() override;
    const char* getSourceName() const override;
    Value serialize(
        boost::optional<ExplainOptions::Verbosity> explain = boost::none) const override;

    StageConstraints constraints(Pipeline::SplitState pipeState) const final {
        return {StreamType::kStreaming,
                PositionRequirement::kNone,
                HostTypeRequirement::kLocalOnly,
                DiskUseRequirement::kNoDiskUse,
                FacetRequirement::kAllowed,
                TransactionRequirement::kNotAllowed,
                LookupRequirement::kAllowed,
                UnionRequirement::kAllowed};
    }

    boost::optional<DistributedPlanLogic> distributedPlanLogic() final {
        return boost::none;
    }

    void reattachToOperationContext(OperationContext* opCtx) {
        _opCtx = opCtx; 
    }

    void detachFromOperationContext() {
        _opCtx = nullptr; 
    }

    boost::intrusive_ptr<DocumentSource> optimize() override {
        return this;
    }
    
protected:
    void doDispose() override;

private:
    struct State;
    struct KeyFieldSpec {
        std::string table;
        std::string field;

        BSONObj toBSON() const;
    };

    struct KeyFieldValue {
        explicit KeyFieldValue(const Document& doc);
        explicit KeyFieldValue(std::string table, std::string field, Value value)
            : table(std::move(table)), field(std::move(field)), value(std::move(value)) {}

        Document toDocument() const;

        bool operator==(const KeyFieldValue& other) const;
        bool operator<(const KeyFieldValue& other) const;

        std::string table;
        std::string field;
        Value value;
    };

    DocumentSourceOracle(const boost::intrusive_ptr<ExpressionContext>& expCtx,
                         DocumentSourceOracleSpec spec,
                         std::string sqlQuery,
                         std::vector<KeyFieldSpec> keyFields);

    Value _oracleToValue(size_t idx);

    void _setupMergeWithState(const Document& currentSourceDoc, State* state);
    std::unique_ptr<State> _buildOracleState(const boost::optional<Document>& currentSourceDoc);
    Document _fetchRowIntoDocument(std::vector<KeyFieldValue>* interestingFields);
    GetNextResult _doGetMergedNext();

    OperationContext* _opCtx = nullptr;

    DocumentSourceOracleSpec _spec; 
    std::string _sqlQuery;
    std::vector<KeyFieldSpec> _keyFields;

    std::unique_ptr<State> _state;
};

} // namespace mongo::migrator
