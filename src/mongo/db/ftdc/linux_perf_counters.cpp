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

#include "mongo/base/init.h"
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
#include <sys/resource.h>
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
    std::make_tuple("cpu_migrations"_sd, PERF_TYPE_SOFTWARE, PERF_COUNT_SW_CPU_MIGRATIONS),
    std::make_tuple("page_faults_major"_sd, PERF_TYPE_SOFTWARE, PERF_COUNT_SW_PAGE_FAULTS_MAJ),
    std::make_tuple("page_faults_minor"_sd, PERF_TYPE_SOFTWARE, PERF_COUNT_SW_PAGE_FAULTS_MIN)};

static std::vector<std::pair<int, StringData>> counters;

}  // namespace

class LinuxPerfCounterCollector final : public FTDCCollectorInterface {
    LinuxPerfCounterCollector(LinuxPerfCounterCollector&) = delete;
    LinuxPerfCounterCollector& operator=(LinuxPerfCounterCollector&) = delete;

public:
    LinuxPerfCounterCollector() {
    }

    ~LinuxPerfCounterCollector() {
        for (auto pair: counters) {
            close(pair.first);
        }
    }

    std::string name() const final {
        return "perfCounters";
    }

    void collect(OperationContext* opCtx, BSONObjBuilder& builder) {
        for (const auto& kv: counters) {
            long long counter = 0;
            ssize_t size = ::read(kv.first, &counter, sizeof(counter));
            if (size == -1) {
                warning() << "Error reading perf counter: " << errnoWithDescription();
                continue;
            }

            builder << kv.second << counter;
        }

        struct rusage usage = {};
        getrusage(RUSAGE_SELF, &usage);
        builder << "voluntary_context_switches" << usage.ru_nvcsw
                << "involuntary_context_switches" << usage.ru_nivcsw;
    }
};

void installLinuxPerfCounters(FTDCController* controller) {
    controller->addPeriodicCollector(stdx::make_unique<LinuxPerfCounterCollector>());
}

MONGO_INITIALIZER(setupPerfCounters)(::mongo::InitializerContext* ctx) {
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
        pe.inherit = 1;
        pe.disabled = leaderFd == -1 ? 1 : 0;

        auto fd = perf_event_open(&pe, 0, -1, leaderFd, 0);
        if (fd == -1) {
            warning() << "Error opening perf counter " << name << ": "
                      << errnoWithDescription();
            continue;
        }

        if (leaderFd == -1) {
            leaderFd = fd;
        }

        counters.emplace_back(std::make_pair(fd, name));
    }

    ioctl(leaderFd, PERF_EVENT_IOC_RESET, 0);
    ioctl(leaderFd, PERF_EVENT_IOC_ENABLE, 0);

    return Status::OK();
}

}  // namespace mongo
