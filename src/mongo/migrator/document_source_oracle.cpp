#include "mongo/bson/bsonmisc.h"
#define MONGO_LOGV2_DEFAULT_COMPONENT ::mongo::logv2::LogComponent::kDefault

#include "mongo/platform/basic.h"

#include "mongo/migrator/document_source_oracle.h"

#include <set>
#include <string_view>

#include "boost/smart_ptr/intrusive_ptr.hpp"

#include "dpi.h"

#include "mongo/base/error_codes.h"
#include "mongo/base/string_data.h"
#include "mongo/bson/bsonelement.h"
#include "mongo/bson/bsonobjbuilder.h"
#include "mongo/bson/bsontypes.h"
#include "mongo/db/exec/document_value/document.h"
#include "mongo/db/exec/document_value/value.h"
#include "mongo/db/pipeline/document_source.h"
#include "mongo/db/pipeline/expression_context.h"
#include "mongo/idl/idl_parser.h"
#include "mongo/logv2/log.h"
#include "mongo/migrator/document_source_oracle_gen.h"
#include "mongo/migrator/oracle_conn_manager.h"
#include "mongo/migrator/oracle_helpers.h"
#include "mongo/util/str.h"

namespace mongo::migrator {

REGISTER_DOCUMENT_SOURCE(oracle,
        LiteParsedDocumentSourceDefault::parse, DocumentSourceOracle::createFromBson);

struct DocumentSourceOracle::State {
    State(OracleConnectionManager* connManager, const std::string& sqlQuery)
        : conn(connManager->acquireConnection()), stmt(conn.prepareStatement(sqlQuery)) {}

    OracleConnection conn;
    OracleStatement stmt;
    std::vector<OracleColumnInfo> columnInfo;
    std::vector<std::string> lowerCaseColumnNames;
    boost::optional<OracleVariable> mergeFieldName;
    boost::optional<OracleVariable> mergeFieldValue;
    Document currentSourceDoc;
};

DocumentSourceOracle::DocumentSourceOracle(
        const boost::intrusive_ptr<ExpressionContext>& expCtx,
        DocumentSourceOracleSpec spec,
        std::string sqlQuery,
        std::vector<KeyFieldSpec> keyFields)
    : DocumentSource(kStageName, expCtx),
      _opCtx(expCtx->opCtx),
      _spec(std::move(spec)),
      _sqlQuery(std::move(sqlQuery)),
      _keyFields(std::move(keyFields))
{}

BSONObj DocumentSourceOracle::KeyFieldSpec::toBSON() const {
    return BSON("table" << table << "field" << field);
}

OracleFieldSpec OracleFieldSpec::parseFromBSON(const BSONElement &elem) {
    if (elem.type() == String) {
        return OracleFieldSpec(elem.String());
    }
    uassert(ErrorCodes::BadValue,
            "oracle field spec must be either a string or a field spec struct", elem.isABSONObj());
    const auto subObj = elem.Obj();
    std::string name, alias;
    for (const auto& field: elem.Obj()) {
        if (field.fieldNameStringData() == "name"_sd) {
            name = field.String();
        } else if (field.fieldNameStringData() == "alias"_sd) {
            alias = field.String();
        } else {
            uasserted(ErrorCodes::BadValue, "oracle field spec may only contain name and alias fields");
        }
    }
    uassert(ErrorCodes::BadValue,
            "name field of oracle field spec must not be empty", name.empty());
    uassert(ErrorCodes::BadValue,
            "alias field of oracle field spec must not be empty", alias.empty());
    return OracleFieldSpec(std::move(name), std::move(alias));
}

void OracleFieldSpec::serializeToBSON(StringData fieldName, BSONObjBuilder* bob) const {
    if (_alias.empty()) {
        bob->append(fieldName, _name);
    } else {
        BSONObjBuilder subObj(bob->subobjStart(fieldName));
        subObj << "name" << _name << "alias" << _alias;
        subObj.doneFast();
    }
}

void OracleFieldSpec::serializeToBSON(BSONArrayBuilder* bob) const {
    if (_alias.empty()) {
        bob->append(_name);
    } else {
        BSONObjBuilder subObj(bob->subobjStart());
        subObj << "name" << _name << "alias" << _alias;
        subObj.doneFast();
    }
}

std::string OracleFieldSpec::toString(boost::optional<StringData> tableName) const {
    auto fullName = [&] {
        if (tableName) {
            return fmt::format("{}.{}", _name, *tableName);
        } else {
            return _name;
        }
    }();
    if (_alias.empty()) {
        return fullName;
    }
    return fmt::format("{} AS {}", fullName, _alias);
}

boost::intrusive_ptr<DocumentSource> DocumentSourceOracle::createFromBson(
        BSONElement elem, const boost::intrusive_ptr<ExpressionContext>& expCtx) {
    IDLParserErrorContext idlErrCtx(kStageName);
    auto spec = DocumentSourceOracleSpec::parse(idlErrCtx, elem.embeddedObject());

    std::vector<std::string> fields;
    std::vector<std::string> joins;
    std::vector<KeyFieldSpec> keyFields;
    auto topLevelFields = spec.getFields();
    if (auto joinSpecs = spec.getJoins(); joinSpecs) {
        if (topLevelFields) {
            for (const auto& field: *topLevelFields) {
                fields.push_back(field.toString(spec.getTable()));
            }
        } else {
            fields.push_back(fmt::format("{}.*", spec.getTable()));
        }

        for (const auto& joinSpec : *joinSpecs) {
            auto foreignField = fmt::format(
                    "{}.{}", joinSpec.getForeignTable(), joinSpec.getForeignKey());
            if (auto joinFields = joinSpec.getFields(); joinFields) {
                for (const auto& field: *joinFields) {
                    fields.push_back(field.toString(joinSpec.getForeignTable()));
                }
            } else {
                fields.push_back(fmt::format("{}.*", joinSpec.getForeignTable()));
            }

            auto joinName = [&]() -> StringData {
                if (joinSpec.getType() == OracleJoinTypeEnum::kInner) {
                    return "INNER";
                } else if (joinSpec.getType() == OracleJoinTypeEnum::kLeftOuter) {
                    return "LEFT OUTER";
                } else if (joinSpec.getType() == OracleJoinTypeEnum::kRightOuter) {
                    return "RIGHT OUTER";
                } else if (joinSpec.getType() == OracleJoinTypeEnum::kFullOuter) {
                    return "FULL OUTER";
                }
                MONGO_UNREACHABLE;
            }();

            StringData localKey = joinSpec.getLocalKey() ?
                *joinSpec.getLocalKey() : spec.getPrimaryKey();

            joins.push_back(fmt::format("{} JOIN {} ON {} = {}.{}",
                        joinName,
                        joinSpec.getForeignTable(),
                        foreignField,
                        joinSpec.getLocalTable().value_or(spec.getTable()),
                        localKey));

            keyFields.push_back(
                    {joinSpec.getForeignTable().toString(), joinSpec.getForeignKey().toString()});
        }
    } else {
        if (topLevelFields) {
            std::transform(
                    topLevelFields->begin(),
                    topLevelFields->end(),
                    std::back_inserter(fields),
                    [](const auto& field) {
                return field.toString(boost::none);
            });
        }
    }

    keyFields.push_back({spec.getTable().toString(), spec.getPrimaryKey().toString()});

    if (fields.empty()) {
        fields.push_back("*");
    }

    auto sqlQuery = [&] {
        auto fieldsJoined = fmt::join(fields, ", ");
        auto joinsJoined = fmt::join(joins, " ");
        if (spec.getMergeWith()) {
            if (spec.getMergeWith()->getType() == OracleMergeTypeEnum::kOneToOne) { 
                return fmt::format(
                        "SELECT {} FROM {} {} WHERE ?: = ?: AND ROWNUM <= 1",
                        std::move(fieldsJoined),
                        spec.getTable(),
                        std::move(joinsJoined));
            }
            return fmt::format(
                    "SELECT {} FROM {} {} WHERE ?: = ?:",
                    std::move(fieldsJoined),
                    spec.getTable(),
                    std::move(joinsJoined));
        }
        return fmt::format(
                "SELECT {} FROM {} {}",
                std::move(fieldsJoined),
                spec.getTable(),
                std::move(joinsJoined));
    }();

    LOGV2(0, "Making $oracle pipeline stage", "sqlQuery"_attr = sqlQuery, "keyFields"_attr = keyFields);
    return boost::intrusive_ptr<DocumentSourceOracle>(new DocumentSourceOracle(
                expCtx, std::move(spec), std::move(sqlQuery), std::move(keyFields)));
}

const char* DocumentSourceOracle::getSourceName() const {
    return kStageName.rawData();
}

Value DocumentSourceOracle::serialize(boost::optional<ExplainOptions::Verbosity> explain) const {
    return Value(Document({{kStageName, Document(_spec.toBSON())}}));
}

void DocumentSourceOracle::doDispose() {
    _state.reset();
    _opCtx = nullptr; 
}

DocumentSourceOracle::KeyFieldValue::KeyFieldValue(const Document& doc) {
    if (auto field = doc.getField("table"); field.getType() == String) {
        table = field.getString();
    }
    if (auto fieldFromDoc = doc.getField("field"); fieldFromDoc.getType() == String) {
        field = fieldFromDoc.getString();
    }
    if (auto field = doc.getField("value"); !field.missing()) {
        value = field;
    }

    if (table.empty() || field.empty() || value.missing()) {
        uasserted(ErrorCodes::BadValue, "invalid $oracle key fields");
    }
}

Document DocumentSourceOracle::KeyFieldValue::toDocument() const {
    return Document{
        { "table", table },
        { "field", field },
        { "value", value }
    };
}

bool DocumentSourceOracle::KeyFieldValue::operator==(const KeyFieldValue& other) const {
    ValueComparator comparator;
    return table == other.table && field == other.field && comparator.compare(value, other.value) == 0;
}

bool DocumentSourceOracle::KeyFieldValue::operator<(const KeyFieldValue& other) const {
    ValueComparator comparator;
    return table < other.table && field < other.field && comparator.compare(value, other.value) < 0;
}

DocumentSource::GetNextResult DocumentSourceOracle::_doGetMergedNext() {
    std::vector<Document> mergedResults;
    std::vector<KeyFieldValue> keyFields;
    while (_state->stmt.fetch()) {
        mergedResults.push_back(_fetchRowIntoDocument(&keyFields));
    }

    MutableDocument outDocument(_state->currentSourceDoc);
    if (mergedResults.size() == 0) {
        outDocument.setNestedField(_spec.getMergeWith()->getTargetField(), Value(BSONNULL));
    } else if(_spec.getMergeWith()->getType() == OracleMergeTypeEnum::kOneToOne) {
        outDocument.setNestedField(_spec.getMergeWith()->getTargetField(),
                Value(std::move(mergedResults.front())));
    } else {
        outDocument.setNestedField(_spec.getMergeWith()->getTargetField(),
                Value(std::move(mergedResults)));
    }

    Value theirKeyFields = _state->currentSourceDoc.getField("__mongo_oracle_keys");
    std::set<KeyFieldValue> mergedKeyFields;

    if (!theirKeyFields.missing()) {
        for (const auto& field: theirKeyFields.getArray()) {
            KeyFieldValue curKeyField(field.getDocument());
            if (std::find(keyFields.begin(), keyFields.end(), curKeyField) == keyFields.end()) {
                keyFields.push_back(curKeyField);
            }
        }
    }

    std::vector<Document> keyFieldDocs;
    keyFieldDocs.reserve(keyFields.size());
    std::transform(
            keyFields.begin(),
            keyFields.end(),
            std::back_inserter(keyFieldDocs),
            [](const KeyFieldValue& value) { return value.toDocument(); });

    outDocument.setField("__mongo_oracle_keys", Value(std::move(keyFieldDocs)));
    return outDocument.freeze();
}

Document DocumentSourceOracle::_fetchRowIntoDocument(std::vector<KeyFieldValue>* interestingFields) {
    MutableDocument outputDocument;
    for (size_t idx = 0; idx < _state->columnInfo.size(); ++idx) {
        const auto& fieldName = _state->columnInfo[idx].name();
        auto translatedValue = _oracleToValue(idx);
        outputDocument.addField(fieldName, translatedValue);
        if (auto it = std::find_if(_keyFields.begin(), _keyFields.end(), [&](const auto& field) {
                    return field.field == _state->lowerCaseColumnNames[idx];
            }); it != _keyFields.end()) {
            interestingFields->push_back(KeyFieldValue(it->table, it->field, translatedValue));
        }
    }
    return outputDocument.freeze();
}

DocumentSource::GetNextResult DocumentSourceOracle::doGetNext() try {
    invariant(_opCtx);

    if (!_state) {
        boost::optional<Document> currentSourceDoc;
        if (_spec.getMergeWith()) {
            auto upstreamDoc = pSource->getNext();
            if (!upstreamDoc.isAdvanced()) {
                return upstreamDoc;
            }

            currentSourceDoc = upstreamDoc.releaseDocument();
        }

        _state = _buildOracleState(currentSourceDoc);
        if (!_state) {
            uasserted(0, "No Oracle connections were configured for $oracle pipeline stage");
        }
    } else if (_spec.getMergeWith()) {
        auto upstreamDoc = pSource->getNext();
        if (!upstreamDoc.isAdvanced()) {
            return upstreamDoc;
        }

        _setupMergeWithState(upstreamDoc.getDocument(), _state.get());
    }

    if (_spec.getMergeWith()) {
        return _doGetMergedNext();
    }

    if (!_state->stmt.fetch()) {
        return DocumentSource::GetNextResult::makeEOF();
    }

    std::vector<KeyFieldValue> keyFields;
    MutableDocument outputDocument(_fetchRowIntoDocument(&keyFields));
    std::vector<Document> keyFieldDocs;
    keyFieldDocs.reserve(keyFields.size());
    std::transform(
            keyFields.begin(),
            keyFields.end(),
            std::back_inserter(keyFieldDocs),
            [](const KeyFieldValue& value) { return value.toDocument(); });
    outputDocument.setField("__mongo_oracle_keys", Value{std::move(keyFieldDocs)});

    return outputDocument.freeze();

} catch(const OracleException& e) {
    uasserted(ErrorCodes::InternalError, e.what());
}

void DocumentSourceOracle::_setupMergeWithState(const Document& currentSourceDoc, State* state) {
    boost::optional<OracleVariable> mergeField, mergeValue;
    OracleConnection::VariableOpts fieldVarOpts;
    fieldVarOpts.dbTypeNum = DPI_ORACLE_TYPE_NVARCHAR;
    fieldVarOpts.nativeTypeNum = DPI_NATIVE_TYPE_BYTES;
    fieldVarOpts.maxArraySize = _spec.getMergeWith()->getForeignKey().size();
    mergeField.emplace(state->conn.newArrayVariable(fieldVarOpts));
    mergeField->setFrom(1, _spec.getMergeWith()->getForeignKey());
    state->stmt.bindByPos(1, *mergeField);

    auto localFieldKey = _spec.getMergeWith()->getLocalKey().value_or(_spec.getPrimaryKey());

    auto mergeValueFromDoc = currentSourceDoc.getNestedField(localFieldKey);
    uassert(ErrorCodes::BadValue,
            "source documents for $oracle stage with mergeWith option must have a value "
            "defined for the field to merge on",
            !mergeValueFromDoc.missing());

    auto mergeValueStr = mergeValueFromDoc.coerceToString();

    OracleConnection::VariableOpts valueVarOpts;
    fieldVarOpts.dbTypeNum = DPI_ORACLE_TYPE_NVARCHAR;
    fieldVarOpts.nativeTypeNum = DPI_NATIVE_TYPE_BYTES;
    fieldVarOpts.maxArraySize = static_cast<uint32_t>(mergeValueStr.size());
    mergeValue.emplace(state->conn.newArrayVariable(fieldVarOpts));
    mergeValue->setFrom(1, mergeValueStr);
    state->stmt.bindByPos(1, *mergeValue);
    state->stmt.execute();

    state->currentSourceDoc = currentSourceDoc; 
}

std::unique_ptr<DocumentSourceOracle::State> DocumentSourceOracle::_buildOracleState(
        const boost::optional<Document>& currentSourceDoc) {
    invariant(_opCtx);
    auto connManager = OracleConnectionManager::get(_opCtx->getServiceContext());
    if (!connManager) {
        return nullptr;
    }

    auto state = std::make_unique<State>(connManager, _sqlQuery);

    boost::optional<OracleVariable> mergeValue;
    if (_spec.getMergeWith()) {
        uassert(ErrorCodes::InternalError,
                "cannot have $oracle stage with mergeWith option without a source document",
                currentSourceDoc);

        _setupMergeWithState(*currentSourceDoc, state.get());
    } else {
        state->stmt.execute();
    }
    std::vector<OracleColumnInfo> columnInfo;
    std::vector<std::string> lowerCaseColumnNames;
    columnInfo.reserve(state->stmt.numColumns());
    for (uint32_t idx = 1; idx <= state->stmt.numColumns(); ++idx) {
        columnInfo.push_back(state->stmt.getColumnInfo(idx));
        lowerCaseColumnNames.push_back(str::toLower(columnInfo.back().name()));
    }

    return state;
}

Value DocumentSourceOracle::_oracleToValue(size_t idx) {
    auto datum = _state->stmt.getColumnValue(idx + 1);
    switch(_state->columnInfo[idx].typeInfo().defaultNativeTypeNum) {
    case DPI_NATIVE_TYPE_BOOLEAN:
        return Value(datum.as<bool>());
    case DPI_NATIVE_TYPE_DOUBLE:
        return Value(datum.as<double>());
    case DPI_NATIVE_TYPE_FLOAT:
        return Value(static_cast<double>(datum.as<float>()));
    case DPI_NATIVE_TYPE_INT64:
        return Value(datum.as<int64_t>());
    case DPI_NATIVE_TYPE_UINT64:
        return Value(Decimal128(datum.as<uint64_t>()));
    case DPI_NATIVE_TYPE_BYTES:
        return Value(StringData(datum.as<std::string_view>()));
    default:
        return Value();
    }
}

} // namespace mongo::migrator
