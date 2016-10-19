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

#include "mongo/base/data_range_cursor.h"
#include "mongo/base/data_type_endian.h"
#include "mongo/base/init.h"
#include "mongo/stdx/memory.h"
#include "mongo/transport/message_compressor_manager.h"
#include "mongo/transport/message_compressor_registry.h"
#include "mongo/transport/message_compressor_zstd.h"

#include "third_party/zstd-1.1.0/lib/dictBuilder/zdict.h"
#include "third_party/zstd-1.1.0/lib/zstd.h"

namespace mongo {
namespace {
const auto kDictionarySize = 1024 * 100;
const auto kTargetSampleSize = kDictionarySize * 100;

class ZStdState {
public:
    ZStdState()
        : samples(kTargetSampleSize, 0), sampleSizes(), dictionaryBytes(kDictionarySize, 0) {
        compressionContext = decltype(compressionContext)(
            ZSTD_createCCtx(), [](ZSTD_CCtx* ptr) { ZSTD_freeCCtx(ptr); });
        decompressionContext = decltype(decompressionContext)(
            ZSTD_createDCtx(), [](ZSTD_DCtx* ptr) { ZSTD_freeDCtx(ptr); });
    }

    //    ZStdState(ZStdState&&) = default;
    //    ZStdState& operator=(ZStdState&&) = default;

    bool sentMyDictionary = false;
    std::unique_ptr<ZSTD_CDict, std::function<void(ZSTD_CDict*)>> myParsedDictionary = nullptr;
    std::unique_ptr<ZSTD_DDict, std::function<void(ZSTD_DDict*)>> theirParsedDictionary = nullptr;
    std::unique_ptr<ZSTD_CCtx, std::function<void(ZSTD_CCtx*)>> compressionContext = nullptr;
    std::unique_ptr<ZSTD_DCtx, std::function<void(ZSTD_DCtx*)>> decompressionContext = nullptr;

    std::vector<uint8_t> samples;
    std::vector<size_t> sampleSizes;
    std::vector<uint8_t> dictionaryBytes;
    size_t curSampleSize = 0;

    void appendSample(ConstDataRange input) {
        if (myParsedDictionary)
            return;

        if (curSampleSize + input.length() > samples.size()) {
            samples.resize(curSampleSize + input.length());
        }

        memcpy(samples.data() + curSampleSize, input.data(), input.length());
        curSampleSize += input.length();

        sampleSizes.push_back(input.length());

        if (sampleSizes.size() > 1000 && curSampleSize >= kTargetSampleSize) {
            auto realDictSize = ZDICT_trainFromBuffer(dictionaryBytes.data(),
                                                      dictionaryBytes.size(),
                                                      samples.data(),
                                                      sampleSizes.data(),
                                                      sampleSizes.size());

            uassert(ErrorCodes::ZStdError,
                    ZDICT_getErrorName(realDictSize),
                    ZDICT_isError(realDictSize) == 0);
            dictionaryBytes.resize(realDictSize);

            myParsedDictionary = decltype(myParsedDictionary)(
                ZSTD_createCDict(dictionaryBytes.data(), dictionaryBytes.size(), 0),
                [](ZSTD_CDict* ptr) { ZSTD_freeCDict(ptr); });
            uassert(
                ErrorCodes::ZStdError, "Error creating digested dictionary", myParsedDictionary);

            samples.clear();
            sampleSizes.clear();
            curSampleSize = 0;
        }
    }
};

const auto getState = MessageCompressorManager::declareDecoration<ZStdState>();
}

ZstdMessageCompressor::ZstdMessageCompressor() : MessageCompressorBase(MessageCompressor::kZStd) {}

std::size_t ZstdMessageCompressor::getMaxCompressedSize(MessageCompressorManager* manager,
                                                        size_t inputSize) {
    auto& state = getState(manager);

    auto dataSize = ZSTD_compressBound(inputSize) + sizeof(uint8_t);
    if (state.myParsedDictionary && !state.sentMyDictionary) {
        return dataSize + state.dictionaryBytes.size();
    }

    return dataSize;
}

StatusWith<std::size_t> ZstdMessageCompressor::compressData(MessageCompressorManager* manager,
                                                            ConstDataRange input,
                                                            DataRange output) {
    auto& state = getState(manager);
    state.appendSample(input);
    DataRangeCursor outputCursor(const_cast<char*>(output.data()),
                                 const_cast<char*>(output.data()) + output.length());

    size_t outLength;
    if (!state.myParsedDictionary) {
        outputCursor.writeAndAdvance<LittleEndian<uint8_t>>(0);

        outLength = ZSTD_compressCCtx(state.compressionContext.get(),
                                      const_cast<char*>(outputCursor.data()),
                                      outputCursor.length(),
                                      input.data(),
                                      input.length(),
                                      0);

        if (ZSTD_isError(outLength)) {
            return Status{ErrorCodes::ZStdError, ZSTD_getErrorName(outLength)};
        }

        outLength += 1;
    } else if (!state.sentMyDictionary) {
        outputCursor.writeAndAdvance<LittleEndian<uint8_t>>(1);
        outputCursor.writeAndAdvance<LittleEndian<size_t>>(state.dictionaryBytes.size());
        memcpy(const_cast<char*>(outputCursor.data()),
               state.dictionaryBytes.data(),
               state.dictionaryBytes.size());
        outputCursor.advance(state.dictionaryBytes.size());

        outLength = ZSTD_compress_usingCDict(state.compressionContext.get(),
                                             const_cast<char*>(outputCursor.data()),
                                             outputCursor.length(),
                                             input.data(),
                                             input.length(),
                                             state.myParsedDictionary.get());
        if (ZSTD_isError(outLength)) {
            return Status{ErrorCodes::ZStdError, ZSTD_getErrorName(outLength)};
        }

        outLength += state.dictionaryBytes.size() + 1 + sizeof(size_t);
        state.sentMyDictionary = true;
        state.dictionaryBytes.clear();
    } else {
        outputCursor.writeAndAdvance<LittleEndian<uint8_t>>(2);
        outLength = ZSTD_compress_usingCDict(state.compressionContext.get(),
                                             const_cast<char*>(outputCursor.data()),
                                             outputCursor.length(),
                                             input.data(),
                                             input.length(),
                                             state.myParsedDictionary.get());
        if (ZSTD_isError(outLength)) {
            return Status{ErrorCodes::ZStdError, ZSTD_getErrorName(outLength)};
        }
        outLength += 1;
    }

    counterHitCompress(input.length(), outLength);
    return {outLength};
}

StatusWith<std::size_t> ZstdMessageCompressor::decompressData(MessageCompressorManager* manager,
                                                              ConstDataRange input,
                                                              DataRange output) {
    auto& state = getState(manager);
    ConstDataRangeCursor inputCursor(const_cast<char*>(input.data()),
                                     const_cast<char*>(input.data()) + input.length());
    auto packetType = inputCursor.readAndAdvance<LittleEndian<uint8_t>>().getValue();

    size_t outLength;
    switch (packetType) {
        case 0:
            outLength = ZSTD_decompressDCtx(state.decompressionContext.get(),
                                            const_cast<char*>(output.data()),
                                            output.length(),
                                            inputCursor.data(),
                                            inputCursor.length());
            break;
        case 1: {
            auto dictSize = inputCursor.readAndAdvance<LittleEndian<size_t>>().getValue();
            state.theirParsedDictionary = decltype(state.theirParsedDictionary)(
                ZSTD_createDDict(inputCursor.data(), dictSize),
                [](ZSTD_DDict* ptr) { ZSTD_freeDDict(ptr); });
            uassert(ErrorCodes::ZStdError,
                    "Error creating peer's dictionary",
                    state.theirParsedDictionary);
            inputCursor.advance(dictSize);
        }
        case 2:
            outLength = ZSTD_decompress_usingDDict(state.decompressionContext.get(),
                                                   const_cast<char*>(output.data()),
                                                   output.length(),
                                                   inputCursor.data(),
                                                   inputCursor.length(),
                                                   state.theirParsedDictionary.get());
    }

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
