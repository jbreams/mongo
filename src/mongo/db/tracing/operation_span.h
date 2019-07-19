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

#include <memory>

#include <boost/optional.hpp>

#include "mongo/base/string_data.h"
#include "mongo/db/operation_context.h"
#include "mongo/db/tracing/tracing.h"
#include "mongo/util/concurrency/with_lock.h"

namespace mongo {
namespace tracing {

class OperationSpan : public Span, public std::enable_shared_from_this<OperationSpan> {
public:
    OperationSpan(OperationContext* opCtx,
                  std::shared_ptr<tracing::Span> parent,
                  std::unique_ptr<opentracing::Span> span)
        : Span(std::move(span)), _parent(std::move(parent)), _opCtx(opCtx) {}

    static std::shared_ptr<Span> make(OperationContext* opCtx,
                                      StringData name,
                                      std::initializer_list<SpanReference> references = {},
                                      std::shared_ptr<Span> parent = nullptr);
    static std::shared_ptr<Span> getCurrent(OperationContext* opCtx);
    static std::shared_ptr<Span> makeChildOf(OperationContext* opCtx, StringData name);
    static std::shared_ptr<Span> makeFollowsFrom(OperationContext* opCtx, StringData name);
    static std::shared_ptr<Span> initialize(
        OperationContext* opCtx,
        StringData opName,
        boost::optional<SpanReference> parentSpan = boost::none);

    void finish() override;

private:
    static std::shared_ptr<Span> _findTop(OperationContext* opCtx);
    static std::shared_ptr<Span> _initializeImpl(
        WithLock,
        OperationContext* opCtx,
        StringData opName,
        boost::optional<SpanReference> parentSpan = boost::none);

    bool _finished = false;
    std::shared_ptr<Span> _parent = nullptr;
    OperationContext* const _opCtx;
};

}  // namespace tracing
}  // namespace mongo
