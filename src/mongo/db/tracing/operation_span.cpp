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

#include "mongo/platform/basic.h"

#include "mongo/db/tracing/operation_span.h"

#include <stack>

#include "mongo/db/tracing/tracing_setup.h"
#include "mongo/util/decorable.h"

namespace mongo {
namespace tracing {
namespace {

using OperationSpanState = std::stack<std::weak_ptr<Span>>;

const auto getSpanState = OperationContext::declareDecoration<OperationSpanState>();

SpanReference getServiceSpanReference(OperationContext* opCtx) {
    SpanContext* serviceSpanCtx = nullptr;
    if (opCtx && opCtx->getServiceContext()) {
        serviceSpanCtx = const_cast<SpanContext*>(&getServiceSpan(opCtx->getServiceContext())->context());
    } else if (hasGlobalServiceContext()) {
        serviceSpanCtx = const_cast<SpanContext*>(&getServiceSpan(getGlobalServiceContext())->context());
    }

    return tracing::FollowsFrom(serviceSpanCtx);
}

} // namespace

std::shared_ptr<Span> OperationSpan::_findTop(OperationContext* opCtx) {
    auto& spanState = getSpanState(opCtx);
    std::shared_ptr<Span> current;
    while (!spanState.empty() && !current) {
        current = spanState.top().lock();
        if (!current) {
            spanState.pop();
        }
    }

    return current;
}

std::shared_ptr<Span> OperationSpan::getCurrent(OperationContext* opCtx) {
    return _findTop(opCtx);
}

std::shared_ptr<Span> OperationSpan::initialize(OperationContext* opCtx,
                               StringData opName,
                               boost::optional<SpanReference> parentSpan) {
    auto& spanState = getSpanState(opCtx);

    boost::optional<SpanReference> serviceSpanReference;
    if (!spanState.empty()) {
        auto parent = _findTop(opCtx);
        if (parent) {
            serviceSpanReference = tracing::ChildOf(&parent->context());
        }
    }

    std::shared_ptr<Span> span;
    if (!serviceSpanReference) {
        serviceSpanReference = getServiceSpanReference(opCtx);
    }

    if (parentSpan) {
        span = OperationSpan::make(opCtx, opName, { *parentSpan, *serviceSpanReference });
    } else {
        span = OperationSpan::make(opCtx, opName, { *serviceSpanReference });
    }

    spanState.push(span);
    currentOpSpan = span;
    return span;
}

std::shared_ptr<Span> OperationSpan::make(OperationContext* opCtx,
                                          StringData name,
                                          std::initializer_list<SpanReference> references) {
    opentracing::StartSpanOptions options;
    for (auto& ref: references) {
        ref.Apply(options);
    }

    opentracing::string_view svName(name.rawData(), name.size());
    auto span = getTracer().StartSpanWithOptions(svName, options);
    return std::make_shared<OperationSpan>(opCtx, std::move(span));
}

std::shared_ptr<Span> OperationSpan::makeChildOf(OperationContext* opCtx, StringData name) {
        if (currentOpSpan) {
            return OperationSpan::make(nullptr, name, { tracing::ChildOf(&currentOpSpan->context()) });
        } else {
            return OperationSpan::make(nullptr, name, {getServiceSpanReference(opCtx)});
        }
    auto& spanState = getSpanState(opCtx);
    if (spanState.empty()) {
        return initialize(opCtx, name);
    }

    auto parent = _findTop(opCtx);
    auto parentReference = tracing::ChildOf(&parent->context());
    std::shared_ptr<Span> ret(OperationSpan::make(opCtx, name, {parentReference}));
    spanState.push(ret);
    currentOpSpan = ret;

    return ret;
}

std::shared_ptr<Span> OperationSpan::makeFollowsFrom(OperationContext* opCtx, StringData name) {
    if (!opCtx) {
        if (currentOpSpan) {
            return OperationSpan::make(nullptr, name, { tracing::FollowsFrom(&currentOpSpan->context()) });
        } else {
            return OperationSpan::make(nullptr, name, {getServiceSpanReference(opCtx)});
        }
    }

    auto& spanState = getSpanState(opCtx);
    if (spanState.empty()) {
        return initialize(opCtx, name);
    }

    auto parent = _findTop(opCtx);
    auto parentReference = tracing::FollowsFrom(&parent->context());
    std::shared_ptr<Span> ret(OperationSpan::make(opCtx, name, {parentReference}));
    spanState.push(ret);
    currentOpSpan = ret;

    return ret;
}

void OperationSpan::finish() {
    if (finished()) {
        return;
    }

    Span::finish();
    if (!_opCtx) {
        return;
    }

    auto& spanState = getSpanState(_opCtx);
    invariant(!spanState.empty());
    auto top = spanState.top().lock();
    invariant(top == shared_from_this());
    auto newTop = _findTop(_opCtx);
    currentOpSpan = newTop;
}

} // namepsace tracing
} // namespace mongo
