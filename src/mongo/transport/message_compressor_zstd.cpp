/**
 *    Copyright (C) 2016 MongoDB Inc.
 *
 *    This program is free software: you can redistribute it and/or  modify
 *    it under the terms of the GNU Affero General Public License, version 3,
 *    as published by the Free Software Foundation.
 *
 *    This program is distributed in the hope that it will be useful,
 *    but WITHOUT ANY WARRANTY; without even the implied warranty of
 *    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *    GNU Affero General Public License for more details.
 *
 *    You should have received a copy of the GNU Affero General Public License
 *    along with this program.  If not, see <http://www.gnu.org/licenses/>.
 *
 *    As a special exception, the copyright holders give permission to link the
 *    code of portions of this program with the OpenSSL library under certain
 *    conditions as described in each individual source file and distribute
 *    linked combinations including the program with the OpenSSL library. You
 *    must comply with the GNU Affero General Public License in all respects for
 *    all of the code used other than as permitted herein. If you modify file(s)
 *    with this exception, you may extend this exception to your version of the
 *    file(s), but you are not obligated to do so. If you do not wish to do so,
 *    delete this exception statement from your version. If you delete this
 *    exception statement from all source files in the program, then also delete
 *    it in the license file.
 */

#define MONGO_LOG_DEFAULT_COMPONENT ::mongo::logger::LogComponent::kNetwork

#include "mongo/platform/basic.h"

#include "mongo/base/init.h"
#include "mongo/stdx/memory.h"
#include "mongo/transport/message_compressor_registry.h"
#include "mongo/transport/message_compressor_zstd.h"

#include "third_party/zstd-1.1.0/lib/zstd.h"

namespace mongo {

ZstdMessageCompressor::ZstdMessageCompressor() : MessageCompressorBase(MessageCompressor::kZStd) {}

std::size_t ZstdMessageCompressor::getMaxCompressedSize(size_t inputSize) {
    return ZSTD_compressBound(inputSize);
}

StatusWith<std::size_t> ZstdMessageCompressor::compressData(ConstDataRange input,
                                                            DataRange output) {
    size_t outLength = ZSTD_compress(
        const_cast<char*>(output.data()), output.length(), input.data(), input.length(), 0);

    if (ZSTD_isError(outLength)) {
        return Status{ErrorCodes::ZStdError, ZSTD_getErrorName(outLength)};
    }

    counterHitCompress(input.length(), outLength);
    return {outLength};
}

StatusWith<std::size_t> ZstdMessageCompressor::decompressData(ConstDataRange input,
                                                              DataRange output) {
    size_t outLength = ZSTD_decompress(
        const_cast<char*>(output.data()), output.length(), input.data(), input.length());

    if (ZSTD_isError(outLength)) {
        return Status{ErrorCodes::ZStdError, ZSTD_getErrorName(outLength)};
    }

    counterHitDecompress(input.length(), outLength);
    return {outLength};
}


MONGO_INITIALIZER_GENERAL(ZstdMessageCompressorInit,
                          ("EndStartupOptionHandling"),
                          ("AllCompressorsRegistered"))
(InitializerContext* context) {
    auto& compressorRegistry = MessageCompressorRegistry::get();
    compressorRegistry.registerImplementation(stdx::make_unique<ZstdMessageCompressor>());
    return Status::OK();
}
}  // namespace mongo
