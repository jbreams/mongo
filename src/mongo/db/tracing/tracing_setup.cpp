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

#include "mongo/db/tracing/tracing_setup.h"

#include <opentracing/dynamic_load.h>

#include "mongo/bson/json.h"
#include "mongo/db/service_context.h"
#include "mongo/util/decorable.h"
#include "mongo/util/log.h"

namespace mongo {
namespace {
const auto kJaegerLibraryName = "libjaegertracing.so"_sd;
const auto kTracerConfigFormat = R"(
service_name: {}
disabled: false
reporter:
    logSpans: true
    localAgentHostPort: 10.1.2.24:6831 # JBR's workstation for testing only!
sampler:
  type: const
  param: 1)"_sd;

BSONObj processParentSpan = BSONObj();

const auto getServiceDecoration =
    ServiceContext::declareDecoration<std::unique_ptr<tracing::Span>>();
}  // namepsace

namespace tracing {
std::unique_ptr<tracing::Span>& getServiceSpan(ServiceContext* service) {
    return getServiceDecoration(service);
}

Tracer& getTracer() {
    return *Tracer::Global();
}
}  // namespace tracing


void setupTracing(ServiceContext* service, std::string serviceName) {
    std::string errorMessage;

    static auto handleMaybe =
        opentracing::DynamicallyLoadTracingLibrary(kJaegerLibraryName.rawData(), errorMessage);
    if (!handleMaybe) {
        severe() << "Failed to load tracer library " << kJaegerLibraryName << ": " << errorMessage;
        fassertFailed(31184);
    }

    auto& factory = handleMaybe->tracer_factory();
    auto config = fmt::format(kTracerConfigFormat, serviceName);
    auto tracer = factory.MakeTracer(config.data(), errorMessage);
    if (!tracer) {
        severe() << "Error creating tracer: " << errorMessage;
        fassertFailed(31185);
    }

    opentracing::Tracer::InitGlobal(*tracer);

    std::unique_ptr<tracing::SpanContext> parentSpan;
    if (!processParentSpan.isEmpty()) {
        try {
            auto maybeParentSpan =
                tracing::extractSpanContext(BSON("$spanContext" << processParentSpan));
            if (maybeParentSpan) {
                parentSpan = std::move(*maybeParentSpan);
                log() << "Extracted parent tracing span from command line options: "
                      << processParentSpan;
            }
        } catch (const DBException& e) {
            fassertFailedWithStatus(
                51244, e.toStatus().withContext("Failed to extract process parent span"));
        }
    }

    std::unique_ptr<tracing::Span> rootSpan;
    if (parentSpan) {
        rootSpan = tracing::Span::make(serviceName, {tracing::FollowsFrom(parentSpan.get())});
    } else {
        rootSpan = tracing::Span::make(serviceName);
    }

    tracing::getServiceSpan(service) = std::move(rootSpan);

    log() << "initialized opentracing";
}

void shutdownTracing(ServiceContext* service) {
    auto serviceSpan = std::move(tracing::getServiceSpan(service));
    if (!serviceSpan) {
        return;
    }
    serviceSpan->finish();
    tracing::getTracer().Close();
}

Status setProcessParentSpan(const std::string& value) try {
    processParentSpan = fromjson(value);
    return Status::OK();
} catch (const DBException& e) {
    return e.toStatus();
}

}  // namespace mongo
