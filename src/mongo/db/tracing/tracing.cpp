/**
 *    Copyright (C) 2019-present MongoDB, Inc.
 *
 *    This program is free software: you can redistribute it and/or modify
 *    it under the terms of the Server Side Public License, version 1,
 *    as published by MongoDB, Inc.
 *
 *    This program is distributed in the hope that it will be useful,
 *    but WITHOUT ANY WARRANTY; without even the implied warranty of
 *    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *    Server Side Public License for more details.
 *
 *    You should have received a copy of the Server Side Public License
 *    along with this program. If not, see
 *    <http://www.mongodb.com/licensing/server-side-public-license>.
 *
 *    As a special exception, the copyright holders give permission to link the
 *    code of portions of this program with the OpenSSL library under certain
 *    conditions as described in each individual source file and distribute
 *    linked combinations including the program with the OpenSSL library. You
 *    must comply with the Server Side Public License in all respects for
 *    all of the code used other than as permitted herein. If you modify file(s)
 *    with this exception, you may extend this exception to your version of the
 *    file(s), but you are not obligated to do so. If you do not wish to do so,
 *    delete this exception statement from your version. If you delete this
 *    exception statement from all source files in the program, then also delete
 *    it in the license file.
 */

#define MONGO_LOG_DEFAULT_COMPONENT ::mongo::logger::LogComponent::kDefault

#include "mongo/platform/basic.h"

#include "mongo/db/tracing/tracing.h"

#include <opentracing/propagation.h>
#include <opentracing/tracer.h>

#include "mongo/bson/json.h"
#include "mongo/util/log.h"

namespace mongo {
namespace {
inline opentracing::string_view fromStringData(StringData data) {
    return opentracing::string_view(data.rawData(), data.size());
}

inline StringData fromStringView(opentracing::string_view view) {
    return StringData(view.data(), view.size());
}

class BSONCarrierReader : public opentracing::TextMapReader {
public:
    explicit BSONCarrierReader(BSONObj obj) : _obj(obj) {}

    opentracing::expected<void> ForeachKey(
        std::function<opentracing::expected<void>(
            opentracing::string_view key, opentracing::string_view value)> func) const override {
        for (const auto& kv : _obj) {
            func(fromStringData(kv.fieldNameStringData()),
                 fromStringData(kv.checkAndGetStringData().rawData()));
        }

        return {};
    }

private:
    BSONObj _obj;
};

class BSONCarrierWriter : public opentracing::TextMapWriter {
public:
    opentracing::expected<void> Set(opentracing::string_view key,
                                    opentracing::string_view value) const override {
        _bob.append(fromStringView(key), fromStringView(value));
        return {};
    }

    BSONObj obj() {
        return _bob.obj();
    }

private:
    mutable BSONObjBuilder _bob;
};

opentracing::Tracer& getTracer() {
    return *opentracing::Tracer::Global();
}

}  // namespace

namespace tracing {
thread_local std::shared_ptr<Span> currentOpSpan = nullptr;

std::unique_ptr<Span> Span::make(StringData name, std::initializer_list<SpanReference> references) {

    opentracing::StartSpanOptions options;
    for (auto& ref : references) {
        ref.Apply(options);
    }

    auto span = getTracer().StartSpanWithOptions(fromStringData(name), options);
    return std::make_unique<Span>(std::move(span));
}

template <class... Ts>
struct overloaded : Ts... {
    using Ts::operator()...;
};
template <class... Ts>
overloaded(Ts...)->overloaded<Ts...>;

void Span::setTag(StringData tagName, const Span::TagValue& value) {
    stdx::visit(overloaded{[&](auto arg) { _span->SetTag(fromStringData(tagName), arg); },
                           [&](StringData arg) {
                               _span->SetTag(fromStringData(tagName), fromStringData(arg));
                           }},
                value);
}

const std::string errorCategoryString(const DBException& error) {
    // there doesn't seem to be a error.category field to switch on...
    if (error.isA<ErrorCategory::NetworkError>()) {
        return "NetworkError";
    }
    if (error.isA<ErrorCategory::Interruption>()) {
        return "Interruption";
    }
    if (error.isA<ErrorCategory::NotMasterError>()) {
        return "NotMasterError";
    }
    if (error.isA<ErrorCategory::StaleShardVersionError>()) {
        return "StaleShardVersion";
    }
    if (error.isA<ErrorCategory::NeedRetargettingError>()) {
        return "NeedRetargettingError";
    }
    if (error.isA<ErrorCategory::WriteConcernError>()) {
        return "WriteConcernError";
    }
    if (error.isA<ErrorCategory::ShutdownError>()) {
        return "ShutdownError";
    }
    if (error.isA<ErrorCategory::CancelationError>()) {
        return "CancelationError";
    }
    if (error.isA<ErrorCategory::ConnectionFatalMessageParseError>()) {
        return "ConnectionFatalMessageParseError";
    }
    if (error.isA<ErrorCategory::ExceededTimeLimitError>()) {
        return "ExceededTimeLimitError";
    }
    if (error.isA<ErrorCategory::SnapshotError>()) {
        return "SnapshotError";
    }
    if (error.isA<ErrorCategory::VoteAbortError>()) {
        return "VoteAbortError";
    }
    if (error.isA<ErrorCategory::NonResumableChangeStreamError>()) {
        return "NonResumableChangeStreamError";
    }
    return "Exception";
}

void Span::logError(const DBException& error) noexcept {
    if (_span) {
        setTag("error", true);
        _span->Log({{"event", "error"},
                    {"error.kind", errorCategoryString(error)},
                    {"error.code", std::string(error.codeString())},
                    {"message", std::string(error.what())}});
    }
}


void Span::log(const Span::LogEntry& item) {
    stdx::visit(overloaded{[&](auto arg) {
                               _span->Log({{fromStringData(item.first), arg}});
                           },
                           [&](StringData arg) {
                               _span->Log({{fromStringData(item.first), fromStringData(arg)}});
                           }},
                item.second);
}

void Span::finish() {
    if (_finished) {
        return;
    }
    _span->Finish();

    _finished = true;
}

void Span::inject(BSONObjBuilder* out) {
    BSONCarrierWriter carrier;
    uassert(51242, "Failed to inject span context", getTracer().Inject(_span->context(), carrier));
    auto obj = carrier.obj();
    out->append("$spanContext", std::move(obj));
}

void Span::setOperationName(StringData name) {
    _span->SetOperationName(fromStringData(name));
}

const SpanContext& Span::context() const {
    return _span->context();
}

Span::~Span() {
    finish();
}

boost::optional<std::unique_ptr<SpanContext>> extractSpanContext(const BSONObj& body) {
    auto elem = body.getField("$spanContext");
    if (elem.eoo()) {
        return boost::none;
    }

    auto expectedSpanContext = getTracer().Extract(BSONCarrierReader(elem.Obj()));
    uassert(51243, "Failed to extract span context", expectedSpanContext);
    return std::move(*expectedSpanContext);
}

}  // namespace tracing
}  // namespace mongo
