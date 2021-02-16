#pragma once

#include "dpi.h"
#include "mpark/variant.hpp"

#include <boost/optional.hpp>

#include <cstddef>
#include <exception>
#include <memory>
#include <stdexcept>
#include <string>
#include <string_view>
#include <vector>

namespace mongo::migrator {

class OracleException : public std::exception {
public:
    explicit OracleException(dpiErrorInfo info, std::string_view context);
    explicit OracleException(std::string context);

    const char* what() const noexcept override;

    const boost::optional<dpiErrorInfo>& info() const;

private:
    const boost::optional<dpiErrorInfo> _errorInfo;
    const std::string _what;
};

class OracleContext {
public:
    OracleContext();
    ~OracleContext();

    const dpiContext* get() const noexcept {
        return _ctx;
    }

    dpiErrorInfo getLastError() const noexcept {
        dpiErrorInfo errInfo;
        dpiContext_getError(_ctx, &errInfo);
        return errInfo;
    }
private:
    dpiContext* _ctx = nullptr;
};

class OracleConnection;
struct OracleConnectionOptions {
    std::string username;
    std::string password;
    std::string connString;
};

class OracleConnectionPool {
public:
    OracleConnectionPool(OracleContext* ctx, const OracleConnectionOptions& opts);

    OracleConnection acquireConnection();

private:
    OracleConnectionPool(const OracleConnectionPool&) = delete;
    OracleConnectionPool(OracleConnectionPool&&) = delete;

    explicit OracleConnectionPool(OracleContext* ctx, dpiPool* pool) : _ctx(ctx), _pool(pool) {}
    OracleContext* _ctx = nullptr;
    dpiPool* _pool = nullptr;
};

class OracleData {
public:
    OracleData() = default;
    OracleData(dpiNativeTypeNum typeNum, dpiData* data)
        : _typeNum(typeNum), _data(data)
    {}

    dpiNativeTypeNum nativeType() const;
    bool isNull() const;

    template <typename T>
    T as() const;

public:
    dpiNativeTypeNum _typeNum = DPI_NATIVE_TYPE_NULL;
    dpiData* _data = nullptr;
};

class OracleStatement;
class OracleRowId {
public:
    OracleRowId(const OracleRowId& other);
    OracleRowId(OracleRowId&& other) noexcept;
    OracleRowId& operator=(const OracleRowId& other);
    OracleRowId& operator=(OracleRowId&& other) noexcept;
    ~OracleRowId();

    operator std::string_view() const {
        return _strValue;
    }

protected:
    friend class OracleVariable;
    operator dpiRowid*() const {
        return _rowId;
    }

private:
    void _initStrValue();

    dpiRowid* _rowId;
    std::string_view _strValue;
};

class OracleVariable {
public: 
    OracleVariable(const OracleVariable& other);
    OracleVariable(OracleVariable&& other) noexcept;
    OracleVariable& operator=(const OracleVariable& other);
    OracleVariable& operator=(OracleVariable&& other) noexcept;
    ~OracleVariable();

    void copyFrom(const OracleVariable& other, uint32_t pos, uint32_t sourcePos);
    void setFrom(uint32_t pos, std::string_view value);
    void setFrom(uint32_t pos, const OracleStatement& stmt);
    void setFrom(uint32_t pos, const OracleRowId& rowId);

    uint32_t numElements() const;
    uint32_t sizeInBytes() const;
    std::vector<OracleData> returnedData(uint32_t pos) const;
    const std::vector<OracleData>& allocatedData() const;

private:
    friend class OracleStatement;
    friend class OracleConnection;
    OracleVariable(OracleContext* ctx, dpiNativeTypeNum nativeType, dpiVar* var, std::vector<OracleData> allocatedData) :
        _ctx(ctx),
        _nativeType(nativeType),
        _var(var),
        _allocatedData(std::move(allocatedData))
    {}

    OracleContext* _ctx;
    dpiNativeTypeNum _nativeType;
    dpiVar* _var;
    std::vector<OracleData> _allocatedData;
};

class OracleConnection {
public:
    static OracleConnection make(OracleContext* ctx, const OracleConnectionOptions& opts);

    OracleConnection(const OracleConnection& other);
    OracleConnection(OracleConnection&& other) noexcept;
    OracleConnection& operator=(const OracleConnection& other);
    OracleConnection& operator=(OracleConnection&& other) noexcept;
    ~OracleConnection();

    OracleStatement prepareStatement(std::string_view sql);
    void commit();

    struct VariableOpts {
        struct ByteBufferOpts {
            uint32_t size;
            bool sizeIsBytes;
        };
        struct ObjectOpts {
            dpiObjectType* objType;
        };

        dpiOracleTypeNum dbTypeNum;
        dpiNativeTypeNum nativeTypeNum;
        uint32_t maxArraySize;
        bool isArray;
        mpark::variant<ByteBufferOpts, ObjectOpts> opts;
    };

    OracleVariable newArrayVariable(VariableOpts opts); 

private:
    friend class OracleConnectionPool;
    explicit OracleConnection(OracleContext* ctx, dpiConn* conn) :
        _ctx(ctx),
        _conn(conn)
    {}

    OracleContext* _ctx;
    dpiConn* _conn = nullptr;
};

class OracleStatement;
class OracleColumnInfo {
public:
    std::string_view name() const noexcept {
        return std::string_view(_info.name, _info.nameLength);
    }

    bool nullOK() const noexcept {
        return _info.nullOk;
    }

    const dpiDataTypeInfo& typeInfo() const noexcept {
        return _info.typeInfo;
    }

private:
    friend class OracleStatement;
    OracleColumnInfo(dpiQueryInfo info)
        : _info(std::move(info))
    {}

    dpiQueryInfo _info;
};

class OracleStatement {
public:
    OracleStatement(const OracleStatement& other);
    OracleStatement(OracleStatement&& other) noexcept;
    OracleStatement& operator=(const OracleStatement& other);
    OracleStatement& operator=(OracleStatement&& other) noexcept;
    ~OracleStatement();

    void execute();
    bool fetch();
    uint32_t numColumns() const;
    OracleColumnInfo getColumnInfo(uint32_t pos) const ;
    OracleData getColumnValue(uint32_t pos) const;

    void bindByPos(uint32_t pos, const OracleVariable& var);

protected:
    friend class OracleConnection;
    OracleStatement(OracleContext* ctx, dpiStmt* statement) :
        _ctx(ctx),
        _statement(statement)
    {}

    friend class OracleVariable;
    operator dpiStmt*() const {
        return _statement;
    }

private:
    std::pair<dpiData*, dpiNativeTypeNum> _dataForColumn(uint32_t pos);

    OracleContext* _ctx = nullptr;
    dpiStmt* _statement = nullptr;
};


} // namespace mongo::migrator
