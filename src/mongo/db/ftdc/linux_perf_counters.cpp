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

#define MONGO_LOG_DEFAULT_COMPONENT ::mongo::logger::LogComponent::kFTDC

#include "mongo/platform/basic.h"

#include <tuple>

#include "mongo/base/status_with.h"
#include "mongo/base/string_data.h"
#include "mongo/bson/bsonobjbuilder.h"
#include "mongo/db/ftdc/collector.h"
#include "mongo/db/ftdc/controller.h"
#include "mongo/stdx/memory.h"
#include "mongo/util/log.h"

#include <asm/unistd.h>
#include <linux/perf_event.h>
#include <sys/ioctl.h>
#include <unistd.h>

namespace mongo {
namespace {
int perf_event_open(
    struct perf_event_attr* hw_event, pid_t pid, int cpu, int group_fd, unsigned long flags) {
    return syscall(__NR_perf_event_open, hw_event, pid, cpu, group_fd, flags);
}

constexpr std::initializer_list<std::tuple<StringData, uint32_t, uint64_t>> kCounterDefs = {
    // Hardware Counters
    std::make_tuple("cache_references"_sd, PERF_TYPE_HARDWARE, PERF_COUNT_HW_CACHE_REFERENCES),
    std::make_tuple("cache_misses"_sd, PERF_TYPE_HARDWARE, PERF_COUNT_HW_CACHE_MISSES),
    std::make_tuple(
        "branch_instructions"_sd, PERF_TYPE_HARDWARE, PERF_COUNT_HW_BRANCH_INSTRUCTIONS),
    std::make_tuple("branch_misses"_sd, PERF_TYPE_HARDWARE, PERF_COUNT_HW_BRANCH_MISSES),
    // Software Counters
    std::make_tuple("context_switches"_sd, PERF_TYPE_SOFTWARE, PERF_COUNT_SW_CONTEXT_SWITCHES),
    std::make_tuple("cpu_migrations"_sd, PERF_TYPE_SOFTWARE, PERF_COUNT_SW_CPU_MIGRATIONS),
    std::make_tuple("page_faults_major"_sd, PERF_TYPE_SOFTWARE, PERF_COUNT_SW_PAGE_FAULTS_MAJ),
    std::make_tuple("page_faults_minor"_sd, PERF_TYPE_SOFTWARE, PERF_COUNT_SW_PAGE_FAULTS_MIN)};

}  // namespace

class LinuxPerfCounterCollector final : public FTDCCollectorInterface {
    LinuxPerfCounterCollector(LinuxPerfCounterCollector&) = delete;
    LinuxPerfCounterCollector& operator=(LinuxPerfCounterCollector&) = delete;

public:
    LinuxPerfCounterCollector() {
        int leaderFd = -1;
        for (const auto& def : kCounterDefs) {
            StringData name;
            int type;
            int config;
            std::tie(name, type, config) = def;

            perf_event_attr pe = {};
            pe.type = type;
            pe.size = sizeof(pe);
            pe.config = config;
            pe.exclude_kernel = 1;
            pe.exclude_hv = 1;
            pe.disabled = 1;
            pe.read_format = PERF_FORMAT_GROUP | PERF_FORMAT_ID;

            auto fd = perf_event_open(&pe, 0, -1, leaderFd, 0);
            if (fd == -1) {
                warning() << "Error opening perf counter " << name << ": "
                          << errnoWithDescription();
                continue;
            }

            if (leaderFd == -1) {
                leaderFd = fd;
            }

            uint64_t counterId;
            ioctl(fd, PERF_EVENT_IOC_ID, &counterId);

            _fds.emplace_back(fd);
            _idsToNames.emplace(counterId, name.toString());
        }

        _readBuffer.resize((_fds.size() * sizeof(read_value)) + sizeof(read_format));
        ioctl(leaderFd, PERF_EVENT_IOC_RESET, PERF_IOC_FLAG_GROUP);
        ioctl(leaderFd, PERF_EVENT_IOC_ENABLE, PERF_IOC_FLAG_GROUP);
    }

    ~LinuxPerfCounterCollector() {
        ioctl(_fds.front(), PERF_EVENT_IOC_DISABLE, PERF_IOC_FLAG_GROUP);
        for (auto fd : _fds) {
            close(fd);
        }
    }

    std::string name() const final {
        return "perfCounters";
    }

    void collect(OperationContext* opCtx, BSONObjBuilder& builder) {
        struct read_format* perfData = reinterpret_cast<read_format*>(_readBuffer.data());

        if (read(_fds.front(), _readBuffer.data(), _readBuffer.size()) == -1) {
            warning() << "Error reading perf counters " << errnoWithDescription();
        }

        for (uint64_t i = 0; i < perfData->nr; i++) {
            long long value = perfData->values[i].value;
            const auto& name = _idsToNames[perfData->values[i].id];
            builder << name << value;
        }
    }

private:
    struct read_value {
        uint64_t value;
        uint64_t id;
    };
    struct read_format {
        uint64_t nr;
        struct read_value values[];
    };

    std::vector<uint8_t> _readBuffer;

    std::vector<int> _fds;
    stdx::unordered_map<uint64_t, std::string> _idsToNames;
};

void installLinuxPerfCounters(FTDCController* controller) {
    controller->addPeriodicCollector(stdx::make_unique<LinuxPerfCounterCollector>());
}

}  // namespace mongo
