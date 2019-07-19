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

#pragma once

#include "mongo/stdx/variant.h"

#include <boost/optional.hpp>
#include <opentracing/span.h>
#include <opentracing/tracer.h>

#include "mongo/base/string_data.h"
#include "mongo/bson/bsonobjbuilder.h"
#include "mongo/stdx/variant.h"

namespace mongo {
namespace tracing {
using SpanContext = opentracing::SpanContext;
using SpanReference = opentracing::SpanReference;

inline SpanReference ChildOf(const SpanContext* span_context) noexcept {
    return opentracing::ChildOf(span_context);
}

inline SpanReference FollowsFrom(const SpanContext* span_context) noexcept {
    return opentracing::ChildOf(span_context);
}

class Span {
public:
    explicit Span(std::unique_ptr<opentracing::Span> span) : _span(std::move(span)) {}
    virtual ~Span();

    static std::unique_ptr<Span> make(StringData name,
                                      std::initializer_list<SpanReference> references = {});

    using TagValue = stdx::variant<StringData, int64_t, int32_t, uint32_t, uint64_t, bool, double>;
    virtual void setTag(StringData tagName, const TagValue& val);

    using LogEntry = std::pair<StringData, TagValue>;
    void log(std::initializer_list<LogEntry> entries) {
        for (auto& entry : entries) {
            log(entry);
        }
    }

    virtual void log(const LogEntry& entry);
    void logError(const DBException& error) noexcept;
    virtual void finish();
    virtual void inject(BSONObjBuilder* bob);
    void setOperationName(StringData name);
    virtual const SpanContext& context() const;

protected:
    bool finished() const {
        return _finished;
    }

private:
    Span() = delete;

    std::unique_ptr<opentracing::Span> _span;
    bool _finished = false;
};

extern thread_local std::shared_ptr<Span> currentOpSpan;

boost::optional<std::unique_ptr<SpanContext>> extractSpanContext(const BSONObj& body);

void logSpanError(const std::unique_ptr<Span>& span, const DBException& error) noexcept;

}  // namespace tracing
}  // namespace mongo
