#pragma once

#include "mongo/bson/bsonelement.h"
#include "mongo/bson/bsonobjbuilder.h"
namespace mongo::migrator {

class OracleFieldSpec {
public:
    explicit OracleFieldSpec(std::string name) : _name(std::move(name)) {}
    OracleFieldSpec(std::string name, std::string alias) :
        _name(std::move(name)), _alias(std::move(alias)) {}

    static OracleFieldSpec parseFromBSON(const BSONElement& elem);
    void serializeToBSON(StringData fieldName, BSONObjBuilder* bob) const;
    void serializeToBSON(BSONArrayBuilder* bob) const;

    const std::string& name() {
        return _name;
    }
    const std::string& alias() {
        return _alias;
    }

    std::string toString(boost::optional<StringData> table) const;

private:
    std::string _name;
    std::string _alias;
};

} // namespace mongo::migrator
