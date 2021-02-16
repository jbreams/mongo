#include "oracle_helpers.h"
#include "dpi.h"
#include "fmt/format.h"
#include <stdexcept>
#include <string_view>

namespace mongo::migrator {

OracleException::OracleException(dpiErrorInfo info, std::string_view context)
    : _errorInfo(std::move(info)),
      _what(fmt::format("{}: {}", context, std::string_view(info.message, info.messageLength)))
{}

OracleException::OracleException(std::string context)
    : _what(std::move(context))
{}

const boost::optional<dpiErrorInfo>& OracleException::info() const {
    return _errorInfo;
}

const char* OracleException::what() const noexcept {
    return _what.c_str();
}

namespace {
void checkErr(int rc, const dpiErrorInfo &errInfo, std::string context) {
  if (rc == DPI_SUCCESS) {
    return;
  }

  throw OracleException(errInfo, std::move(context));
}

void checkErr(int rc, const OracleContext *ctx, std::string context) {
  if (rc == DPI_SUCCESS) {
    return;
  }
  throw OracleException(ctx->getLastError(), std::move(context));
}

void checkErr(bool ok, std::string context) {
    if (!ok) {
        throw OracleException(std::move(context));
    }
}
}

OracleRowId::OracleRowId(const OracleRowId& other) :
    _rowId(other._rowId)
{
    dpiRowid_addRef(_rowId);
    _initStrValue();
}

OracleRowId::OracleRowId(OracleRowId&& other) noexcept :
    _rowId(other._rowId)
{
    other._rowId = nullptr;
    _initStrValue();
}

OracleRowId& OracleRowId::operator=(const OracleRowId& other) {
    if (_rowId != nullptr) {
        dpiRowid_release(_rowId);
    }
    _rowId = other._rowId;
    dpiRowid_addRef(_rowId);
    _initStrValue();
    return *this;
}

OracleRowId& OracleRowId::operator=(OracleRowId&& other) noexcept {
    if (_rowId != nullptr) {
        dpiRowid_release(_rowId);
        _rowId = nullptr;
    }
    std::swap(_rowId, other._rowId);
    _initStrValue();
    return *this;
}

OracleRowId::~OracleRowId() {
    if (_rowId != nullptr) {
        dpiRowid_release(_rowId);
    }
}

void OracleRowId::_initStrValue() {
    const char* strValue = nullptr;
    uint32_t strSize = 0;

    dpiRowid_getStringValue(_rowId, &strValue, &strSize);
    _strValue = std::string_view(strValue, strSize);
}


OracleVariable::OracleVariable(const OracleVariable& other) :
    _ctx(other._ctx),
    _var(other._var)
{
    dpiVar_addRef(_var);
}

OracleVariable::OracleVariable(OracleVariable&& other) noexcept :
    _ctx(other._ctx),
    _var(other._var)
{
    other._var = nullptr;
}

OracleVariable& OracleVariable::operator=(const OracleVariable& other) {
    if (_var != nullptr) {
        dpiVar_release(_var);
    }
    _ctx = other._ctx;
    _var = other._var;
    dpiVar_addRef(_var);
    return *this;
}

OracleVariable& OracleVariable::operator=(OracleVariable&& other) noexcept {
    if (_var != nullptr) {
        dpiVar_release(_var);
        _var = nullptr;
    }
    _ctx = other._ctx;
    std::swap(_var, other._var);
    return *this;
}

OracleVariable::~OracleVariable() {
    if (_var != nullptr) {
        dpiVar_release(_var);
    }
}

void OracleVariable::copyFrom(const OracleVariable &other, uint32_t pos, uint32_t sourcePos) {
    auto rc = dpiVar_copyData(_var, pos, other._var, sourcePos);
    checkErr(rc, _ctx, "copying from variable to variable");
}

void OracleVariable::setFrom(uint32_t pos, std::string_view value) {
    // Maximum size of a string value is 1GB - 2 bytes
    // https://oracle.github.io/odpi/doc/functions/dpiVar.html#c.dpiVar_setFromBytes
   
    if (value.size() >= 1073741822) {
        throw OracleException("cannot set oracle variable from string variable longer than 1GB");
    }
    auto rc = dpiVar_setFromBytes(_var, pos, value.data(), static_cast<uint32_t>(value.size()));
    checkErr(rc, _ctx, "copying from string data to variable");
}

void OracleVariable::setFrom(uint32_t pos, const OracleStatement& stmt) {
    auto rc = dpiVar_setFromStmt(_var, pos, stmt);
    checkErr(rc, _ctx, "copying from statement to variable");
}

void OracleVariable::setFrom(uint32_t pos, const OracleRowId& rowid) {
    auto rc = dpiVar_setFromRowid(_var, pos, rowid);
    checkErr(rc, _ctx, "copying from row id to variable");
}

uint32_t OracleVariable::numElements() const {
    uint32_t res = 0;
    auto rc = dpiVar_getNumElementsInArray(_var, &res);
    checkErr(rc, _ctx, "getting number of elements in oracle variable");
    return res;
}

uint32_t OracleVariable::sizeInBytes() const {
    uint32_t res = 0;
    auto rc = dpiVar_getSizeInBytes(_var, &res);
    checkErr(rc, _ctx, "getting size in bytes of oracle variable");
    return res;
}

std::vector<OracleData> OracleVariable::returnedData(uint32_t pos) const {
    uint32_t numElements = 0;
    dpiData* data = nullptr;

    auto rc = dpiVar_getReturnedData(_var, pos, &numElements, &data);
    checkErr(rc, _ctx, "getting returned data from oracle variable");

    std::vector<OracleData> ret;
    ret.reserve(numElements);
    for (uint32_t idx = 0; idx < numElements; idx++) {
        ret.emplace_back(_nativeType, &data[idx]);
    }

    return ret;
}

const std::vector<OracleData>& OracleVariable::allocatedData() const {
    return _allocatedData;
}

OracleConnection::OracleConnection(const OracleConnection& other) :
    _ctx(other._ctx),
    _conn(other._conn)
{
    dpiConn_addRef(_conn);
}

OracleConnection::OracleConnection(OracleConnection&& other) noexcept :
    _ctx(other._ctx),
    _conn(other._conn)
{
    other._conn = nullptr;
    other._ctx = nullptr;
}

OracleConnection& OracleConnection::operator=(const OracleConnection& other) {
    if (_conn != nullptr) {
        dpiConn_release(_conn);
    }
    _ctx = other._ctx;
    _conn = other._conn;
    dpiConn_addRef(_conn);
    return *this;
}

OracleConnection& OracleConnection::operator=(OracleConnection&& other) noexcept {
    if (_conn != nullptr) {
        dpiConn_release(_conn);
        _conn = nullptr;
    }
    _ctx = nullptr;
    std::swap(_ctx, other._ctx);
    std::swap(_conn, other._conn);
    return *this;
}

OracleConnection::~OracleConnection() {
    if (_conn != nullptr) {
        dpiConn_release(_conn);
    }
}

OracleStatement::OracleStatement(const OracleStatement& other) :
    _ctx(other._ctx),
    _statement(other._statement)
{
    dpiStmt_addRef(_statement);
}

OracleStatement::OracleStatement(OracleStatement&& other) noexcept :
    _ctx(other._ctx),
    _statement(other._statement)
{
    other._statement = nullptr;
    other._ctx = nullptr;
}

OracleStatement& OracleStatement::operator=(const OracleStatement& other) {
    if (_statement != nullptr) {
        dpiStmt_release(_statement);
    }
    _ctx = other._ctx;
    _statement = other._statement;
    dpiStmt_addRef(_statement);
    return *this;
}

OracleStatement& OracleStatement::operator=(OracleStatement&& other) noexcept {
    if (_statement != nullptr) {
        dpiStmt_release(_statement);
        _statement = nullptr;
    }
    _ctx = nullptr;
    std::swap(_ctx, other._ctx);
    std::swap(_statement, other._statement);
    return *this;
}

OracleStatement::~OracleStatement() {
    if (_statement != nullptr) {
        dpiStmt_release(_statement);
    }
}

OracleContext::OracleContext() {
    dpiErrorInfo errInfo;
    auto rc = dpiContext_createWithParams(
        DPI_MAJOR_VERSION,
        DPI_MINOR_VERSION,
        nullptr,
        &_ctx,
        &errInfo
    );
    checkErr(rc, errInfo, "error creating Oracle DPI context");
}

OracleContext::~OracleContext() {
    if(dpiContext_destroy(_ctx) != DPI_SUCCESS) {
        std::abort();
    }
}

OracleConnectionPool::OracleConnectionPool(
        OracleContext* ctx, const OracleConnectionOptions& opts)
    : _ctx(ctx)
{
    dpiPoolCreateParams params;
    dpiContext_initPoolCreateParams(ctx->get(), &params);
    params.waitTimeout = 30000;
    auto rc = dpiPool_create(
            _ctx->get(),
            opts.username.c_str(),
            opts.username.size(),
            opts.password.c_str(),
            opts.password.size(),
            opts.connString.c_str(),
            opts.connString.size(),
            nullptr,
            &params,
            &_pool);
    checkErr(rc, ctx, "error creating oracle connection pool");
}

OracleConnection OracleConnectionPool::acquireConnection() {
    dpiConn* conn;
    int rc = dpiPool_acquireConnection(_pool, nullptr, 0, nullptr, 0, nullptr, &conn);
    checkErr(rc, _ctx, "error acquiring oracle connection");

    return OracleConnection(_ctx, conn);
}

OracleConnection OracleConnection::make(OracleContext *ctx, const OracleConnectionOptions &opts) {
    dpiConn* conn;
    auto rc = dpiConn_create(
            ctx->get(),
            opts.username.c_str(),
            opts.username.size(),
            opts.password.c_str(),
            opts.password.size(),
            opts.connString.c_str(),
            opts.connString.size(),
            nullptr,
            nullptr,
            &conn);

    checkErr(rc, ctx, "error creating oracle connection");

    return OracleConnection(ctx, conn);
}

OracleStatement OracleConnection::prepareStatement(std::string_view sql) {
    dpiStmt* stmt = nullptr;
    int rc = dpiConn_prepareStmt(_conn, 0, sql.data(), sql.size(), nullptr, 0, &stmt);
    checkErr(rc, _ctx, "error preparing oracle statement");

    return OracleStatement(_ctx, stmt);
}

OracleVariable OracleConnection::newArrayVariable(VariableOpts opts) {
    dpiObjectType* objType = nullptr;
    uint32_t size = 0;
    uint32_t sizeIsBytes = 0;

    if (auto bytesOptsPtr = mpark::get_if<VariableOpts::ByteBufferOpts>(&opts.opts)) {
        size = bytesOptsPtr->size;
        sizeIsBytes = bytesOptsPtr->sizeIsBytes;
    } else if (auto objectOptsPtr = mpark::get_if<VariableOpts::ObjectOpts>(&opts.opts)) {
        objType = objectOptsPtr->objType;
    }

    dpiVar* var = nullptr;
    dpiData* data = nullptr;
    auto rc = dpiConn_newVar(
            _conn,
            opts.dbTypeNum,
            opts.nativeTypeNum,
            opts.maxArraySize,
            size,
            sizeIsBytes,
            opts.isArray,
            objType,
            &var,
            &data);
    checkErr(rc, _ctx, "error creating oracle varaible");
    std::vector<OracleData> dataVec;
    dataVec.reserve(opts.maxArraySize);
    for (uint32_t idx = 0; idx < opts.maxArraySize; idx++) {
        dataVec.emplace_back(opts.nativeTypeNum, &data[idx]);
    }
   
    return OracleVariable(_ctx, opts.nativeTypeNum, var, std::move(dataVec));
}

void OracleStatement::execute() {
    int rc = dpiStmt_execute(_statement, DPI_MODE_EXEC_DEFAULT, nullptr);
    checkErr(rc, _ctx, "error executing oracle statement");
}

void OracleConnection::commit() {
    auto rc = dpiConn_commit(_conn);
    checkErr(rc, _ctx, "error committing changes");
}

bool OracleStatement::fetch() {
    int found = 0;
    uint32_t bufferRowIndex;
    auto rc = dpiStmt_fetch(_statement, &found, &bufferRowIndex);
    checkErr(rc, _ctx, "error fetching row from oracle statement");
    return found != 0;
}

uint32_t OracleStatement::numColumns() const {
    uint32_t numColumns;
    auto rc = dpiStmt_getNumQueryColumns(_statement, &numColumns);
    checkErr(rc, _ctx, "error getting column count from Oracle results");
    return numColumns;
}

OracleColumnInfo OracleStatement::getColumnInfo(uint32_t pos) const {
    dpiQueryInfo info;
    int rc = dpiStmt_getQueryInfo(_statement, pos, &info);
    checkErr(rc, _ctx, "error getting column info from oracle results");
    return OracleColumnInfo{std::move(info)};
}

OracleData OracleStatement::getColumnValue(uint32_t pos) const {
    dpiNativeTypeNum typeNum;
    dpiData* data;
    int rc = dpiStmt_getQueryValue(_statement, pos, &typeNum, &data);
    checkErr(rc, _ctx, "error getting column value from oracle results");
    return OracleData(typeNum, data);
}

void OracleStatement::bindByPos(uint32_t pos, const OracleVariable &var) {
    int rc = dpiStmt_bindByPos(_statement, pos, var._var);
    checkErr(rc, _ctx, "binding variable to statement by pos");
}

bool OracleData::isNull() const {
    return dpiData_getIsNull(_data);
}

dpiNativeTypeNum OracleData::nativeType() const {
    return _typeNum;
}

template<>
bool OracleData::as<bool>() const {
    checkErr(_typeNum == DPI_NATIVE_TYPE_BOOLEAN, "value for column is not bool");
    return dpiData_getBool(_data);
}

template<>
int64_t OracleData::as<int64_t>() const {
    checkErr(_typeNum == DPI_NATIVE_TYPE_INT64, "value for column is not int64_t");
    return dpiData_getInt64(_data);
}

template<>
uint64_t OracleData::as<uint64_t>() const {
    checkErr( _typeNum == DPI_NATIVE_TYPE_INT64, "value for column is not uint64_t");
    return dpiData_getUint64(_data);
}

template<>
float OracleData::as<float>() const {
    checkErr(_typeNum == DPI_NATIVE_TYPE_FLOAT, "value for column is not float");
    return dpiData_getFloat(_data);
}

template<>
double OracleData::as<double>() const {
    checkErr(_typeNum == DPI_NATIVE_TYPE_DOUBLE, "value for column is not double");
    return dpiData_getDouble(_data);
}

template<>
dpiTimestamp* OracleData::as<dpiTimestamp*>() const {
    checkErr(_typeNum == DPI_NATIVE_TYPE_TIMESTAMP, "value for column is not timestamp");
    return dpiData_getTimestamp(_data);
}

template<>
std::string_view OracleData::as<std::string_view>() const {
    checkErr(_typeNum == DPI_NATIVE_TYPE_BYTES, "value for column is not bytes");
    auto bytes = dpiData_getBytes(_data);
    return std::string_view(bytes->ptr, bytes->length);
}

} // namespace sqlplusplus
