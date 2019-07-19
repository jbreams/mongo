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

#include "mongo/db/tracing/operation_span.h"

#include <stack>

#include "mongo/db/tracing/tracing_setup.h"
#include "mongo/util/decorable.h"
#include "mongo/util/log.h"

namespace mongo {
namespace tracing {
namespace {

struct OperationSpanState {
    stdx::mutex mutex;
    std::stack<std::weak_ptr<Span>> stack;

    bool empty() {
        return stack.empty();
    }

    void push(std::weak_ptr<Span> span) {
        stack.push(span);
    }

    const std::weak_ptr<Span>& top() const {
        return stack.top();
    }

    void pop() {
        stack.pop();
    }
};

const auto getSpanState = OperationContext::declareDecoration<OperationSpanState>();

SpanReference getServiceSpanReference(OperationContext* opCtx) {
    SpanContext* serviceSpanCtx = nullptr;
    if (opCtx && opCtx->getServiceContext()) {
        serviceSpanCtx =
            const_cast<SpanContext*>(&getServiceSpan(opCtx->getServiceContext())->context());
    } else if (hasGlobalServiceContext()) {
        serviceSpanCtx =
            const_cast<SpanContext*>(&getServiceSpan(getGlobalServiceContext())->context());
    }

    return tracing::FollowsFrom(serviceSpanCtx);
}

}  // namespace

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

std::shared_ptr<Span> OperationSpan::_initializeImpl(WithLock,
                                                     OperationContext* opCtx,
                                                     StringData opName,
                                                     boost::optional<SpanReference> parentSpan) {
    auto& spanState = getSpanState(opCtx);
    boost::optional<SpanReference> serviceSpanReference;
    std::shared_ptr<Span> internalParentSpan;
    if (!spanState.empty()) {
        internalParentSpan = _findTop(opCtx);
        if (internalParentSpan) {
            serviceSpanReference = tracing::ChildOf(&internalParentSpan->context());
        }
    }

    std::shared_ptr<Span> span;
    if (!serviceSpanReference) {
        serviceSpanReference = getServiceSpanReference(opCtx);
    }

    if (parentSpan) {
        span = OperationSpan::make(
            opCtx, opName, {*parentSpan, *serviceSpanReference}, internalParentSpan);
    } else {
        span = OperationSpan::make(opCtx, opName, {*serviceSpanReference}, internalParentSpan);
    }

    spanState.push(span);
    currentOpSpan = span;
    return span;
}

std::shared_ptr<Span> OperationSpan::initialize(OperationContext* opCtx,
                                                StringData opName,
                                                boost::optional<SpanReference> parentSpan) {
    auto& spanState = getSpanState(opCtx);
    stdx::lock_guard<stdx::mutex> lk(spanState.mutex);
    return _initializeImpl(lk, opCtx, opName, parentSpan);
}
std::shared_ptr<Span> OperationSpan::make(OperationContext* opCtx,
                                          StringData name,
                                          std::initializer_list<SpanReference> references,
                                          std::shared_ptr<Span> parent) {
    opentracing::StartSpanOptions options;
    for (auto& ref : references) {
        ref.Apply(options);
    }

    opentracing::string_view svName(name.rawData(), name.size());
    auto span = getTracer().StartSpanWithOptions(svName, options);
    return std::make_shared<OperationSpan>(opCtx, parent, std::move(span));
}

std::shared_ptr<Span> OperationSpan::makeChildOf(OperationContext* opCtx, StringData name) {
    if (!opCtx) {
        if (currentOpSpan) {
            return OperationSpan::make(
                nullptr, name, {tracing::ChildOf(&currentOpSpan->context())}, nullptr);
        } else {
            return OperationSpan::make(nullptr, name, {getServiceSpanReference(opCtx)}, nullptr);
        }
    }
    auto& spanState = getSpanState(opCtx);
    stdx::lock_guard<stdx::mutex> lk(spanState.mutex);

    if (spanState.empty()) {
        return _initializeImpl(lk, opCtx, name);
    }

    auto parent = _findTop(opCtx);
    auto parentReference = tracing::ChildOf(&parent->context());
    std::shared_ptr<Span> ret(OperationSpan::make(opCtx, name, {parentReference}, parent));
    spanState.push(ret);
    currentOpSpan = ret;

    return ret;
}

std::shared_ptr<Span> OperationSpan::makeFollowsFrom(OperationContext* opCtx, StringData name) {
    if (!opCtx) {
        if (currentOpSpan) {
            return OperationSpan::make(
                nullptr, name, {tracing::FollowsFrom(&currentOpSpan->context())}, nullptr);
        } else {
            return OperationSpan::make(nullptr, name, {getServiceSpanReference(opCtx)}, nullptr);
        }
    }

    auto& spanState = getSpanState(opCtx);
    stdx::lock_guard<stdx::mutex> lk(spanState.mutex);
    if (spanState.empty()) {
        return _initializeImpl(lk, opCtx, name);
    }

    auto parent = _findTop(opCtx);
    auto parentReference = tracing::FollowsFrom(&parent->context());
    std::shared_ptr<Span> ret(OperationSpan::make(opCtx, name, {parentReference}, parent));
    spanState.push(ret);
    currentOpSpan = ret;

    return ret;
}

void OperationSpan::finish() {
    if (finished()) {
        return;
    }

    Span::finish();
    if (!_opCtx || !_parent) {
        return;
    }

    auto& spanState = getSpanState(_opCtx);
    stdx::lock_guard<stdx::mutex> lk(spanState.mutex);
    invariant(!spanState.empty());

    std::stack<std::shared_ptr<Span>> tempStorage;
    bool foundSelf = false, foundParent = false;
    const auto thisPtr = shared_from_this();
    while (!spanState.empty()) {
        auto tmpPtr = spanState.top().lock();
        if (!tmpPtr) {
            spanState.pop();
            continue;
        } else if (tmpPtr == thisPtr) {
            spanState.pop();
            foundSelf = true;
        } else if (tmpPtr == _parent) {
            foundParent = true;
            break;
        } else {
            tempStorage.push(std::move(tmpPtr));
            spanState.pop();
        }
    }

    if (!foundParent) {
        warning() << "Coult not find parent when finishing span";
    }
    invariant(foundSelf);
    while (!tempStorage.empty()) {
        spanState.push(tempStorage.top());
        tempStorage.pop();
    }

    currentOpSpan = _findTop(_opCtx);
}

}  // namepsace tracing
}  // namespace mongo
