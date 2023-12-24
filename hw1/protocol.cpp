#include <algorithm>
#include <chrono>
#include <format>
#include <fstream>
#include <iostream>
#include <map>
#include <optional>
#include <set>
#include <stacktrace>
#include <stdint.h>
#include <sstream>
#include <thread>
#include <vector>

#include <pybind11/pybind11.h>
#include <cpptrace/cpptrace.hpp>

#include "binary_format.hpp"

namespace py = pybind11;

using time_ms_t = unsigned long;
using seed_t = uint32_t;

enum class Metadata {
    GENERAL       = 0,
    FINAL_SEGMENT = 1,
    STOP_SENDING  = 2,
    NONE,
};

/* try {
} catch (pybind11::type_error e) {
    std::cout << "Anyhow, any-hollow-fuck:" << std::endl;
    std::cout << e.what() << std::endl;
    while (true) { }
} */

/*
- INTER_IDX:       uint32;
- LENGTH:          uint32;
- SEED:            uint32;
- META:            uint8;
- NR_SEEDS:        uint8;
- ...SEEDS:        ...uint32;
- ?SEGMENT_START:  uint32;
- ?PAYLOAD:        bytes;
*/

template<typename... Args>
auto py_call(py::object object, const char *method, Args&&... args)
{
    // py::gil_scoped_acquire acquire;
    return object.attr(method)(std::forward<Args>(args)...);
}

struct Package {
    uint32_t INTER_IDX, LENGTH, SEED;
    Metadata META;
    uint8_t NR_SEEDS;
    std::vector<uint32_t> SEEDS;
    uint32_t SEGMENT_START;
    Bytes PAYLOAD;

    Decoder parser;

    Package(Bytes raw, int dbg_id = 0)
        : parser(raw)
    {
        INTER_IDX = parser.int32();
        LENGTH = parser.int32();
        SEED = parser.int32();
        META = (Metadata)parser.int8();
        NR_SEEDS = parser.int8();

        for (int _ = 0; _ < NR_SEEDS; ++_)
            SEEDS.push_back(parser.int32());

        SEGMENT_START = -1;
        int nr_left_bytes = LENGTH - 14 - 4 * NR_SEEDS;
        if (nr_left_bytes > 0) {
            SEGMENT_START = parser.int32();
            PAYLOAD = parser.raw_str(nr_left_bytes - 4);
        }
    }
};

std::string log_fmt(const Bytes& s)
{
    std::string res = "[";
    for (int i = 0; i < s.size() && i < 6; i++) {
        if (i)
            res += ' ';
        res += std::to_string(int(s[i]));
    }
    return res + ']';
}

struct PackageWrapper {
    Bytes incomplete;
    PackageWrapper()
    { }

    std::optional<Package> feed(Bytes chunk)
    {
        if (incomplete.empty()) {
            append(incomplete, chunk);
            chunk = std::move(incomplete);
            if (incomplete.size()) throw;
        }

        try {
            return Package(chunk);
        } catch (std::string e) {
            if (e != "not enough input bytes")
                throw e;
            incomplete = std::move(chunk);
        }

        return {};
    }
};

time_ms_t clock_ms()
{
    auto t = std::chrono::steady_clock::now().time_since_epoch();
    return std::chrono::duration_cast<std::chrono::milliseconds>(t).count();
}
time_ms_t time_zero = clock_ms();

std::ofstream fdJ("logJ");
std::ofstream fdB("logB");
std::ofstream fdX("log");

constexpr bool no_log = false;

void logJ(std::string_view s)
{
    fdJ << s << '\n';
    fdX << s << '\n';
}
void logB(std::string_view s)
{
    fdB << s << '\n';
    fdX << s << '\n';
}

struct Segment {
    uint32_t start;
    Bytes value;
    bool is_final;

    Segment(uint32_t start, Bytes value, bool is_final)
        : start(start)
        , value(value)
        , is_final(is_final)
    { }
};


struct Pipe {
    static seed_t seed;
    std::string name;
    seed_t next_seed;
    PackageWrapper package_wrapper;
    std::ofstream& fd;
    py::object channel;

    Pipe(py::object channel_)
        : fd(seed % 2 == 0 ? fdB : fdJ)
    {
        channel = channel_;
        next_seed = (seed % 2 ? 7777 : 555);
        name = (seed % 2 ? "\033[31;1mMr J\033[0m" : "\033[34;1mMr B\033[0m");
        seed++;
    }

    std::optional<Package> incoming(double max_duration = 0.00001)
    {
        // print(f'{clock_ms() - time_zero:.3f} {name}: in \'em, eat \'em', file=fd);
        time_ms_t S_TIME = clock_ms();
        py_call(channel, "set_timeout", max_duration);
        py::bytes pre_chunk;
        try {
            pre_chunk = py_call(channel, "recvfrom", 999999999);
        } catch (...) {
            return {};
        }
        Bytes chunk = pre_chunk;
        std::optional<Package> package = package_wrapper.feed(chunk);
        // print(f'{clock_ms() - time_zero:.3f} {name}: incoming(): {clock_ms() - S_TIME} ms', file=fd);
        return package;
    }

    std::pair<int, Bytes> send_package(std::optional<Segment> segment, std::vector<seed_t>& seeds_to_confirm, uint32_t inter_idx, Metadata meta)
    {
        int o_nr_max_seeds = 30;

        uint32_t INTER_IDX = inter_idx;
        seed_t SEED = next_seed++;
        Metadata META = Metadata::GENERAL;
        if (meta != Metadata::NONE)
            META = meta;
        else if (!segment && segment->is_final)
            META = Metadata::FINAL_SEGMENT;

        int NR_SEEDS = std::min((int)seeds_to_confirm.size(), o_nr_max_seeds);
        int LENGTH = 14 + NR_SEEDS * 4 + (segment ? 4 + segment->value.size() : 0);

        Encoder out;
        out.int32(INTER_IDX);
        out.int32(LENGTH);
        out.int32(SEED);
        out.int8((int)META);
        out.int8(NR_SEEDS);

        for (int i = 0; i < NR_SEEDS; i++) {
            int seed = seeds_to_confirm.back();
            seeds_to_confirm.pop_back();
            out.int32(seed);
        }

        if (segment && segment->value.size()) {
            out.int32(segment->start);
            out.raw_str(segment->value);
        }

        Bytes message = out.load();
        py_call(channel, "sendto", py::bytes(message));
        return std::make_pair(SEED, message);
    }
};
seed_t Pipe::seed = 0;

std::string fmt_seed(seed_t seed, uint32_t inter_idx)
{
    return std::format("\033[32;3mseed=${}!{}\033[0m", seed, inter_idx);
}

int o_retry_ms = 12;
int o_no_hear = 5000;

struct Guard_frequent {
    time_ms_t frequency;
    time_ms_t last_ping;
    Guard_frequent(time_ms_t frequency)
        : frequency(frequency)
        , last_ping(clock_ms())
    { }
    bool bad() { return last_ping + frequency < clock_ms(); }
    void ping() { last_ping = clock_ms(); }
};
struct Freq_remember {
    time_ms_t last_ping;
    Freq_remember()
        : last_ping(clock_ms())
    { }
    time_ms_t since_last_ping() { return clock_ms() - last_ping; }
    void ping() { last_ping = clock_ms(); }
};

#include <cxxabi.h>

std::string currentExceptionTypeName()
{
    int status;
    std::string name = abi::__cxa_demangle(abi::__cxa_current_exception_type()->name(), 0, 0, &status);
    return name;
}

struct WiseProtocol {
    static int nr_nodes;
    std::string name;
    Pipe pipe;
    uint32_t inter_idx = 0;

    void stacktrace()
    {
        auto log_fn = (std::count(name.begin(), name.end(), 'J') ? logJ : logB);
        std::string trace = cpptrace::generate_trace().to_string();
        log_fn(trace);
    }
    void log(std::string s)
    {
        if constexpr (no_log) return;
        std::string timestamp = std::to_string(clock_ms() - time_zero);
        // if (role in no_log_roles) {
        //     return;
        // }
        // std::cout << timestamp << " " << name << " " << s << std::endl;
        if (std::count(name.begin(), name.end(), 'J') != 0) {
            logJ(timestamp + " " + name + " " + s);
        } else {
            logB(timestamp + " " + name + " " + s);
        }
    }

    void log_sent_package(Bytes message)
    {
        if constexpr (no_log) return;

        auto package = PackageWrapper().feed(message);
        if (!package) {
            log("can't parse his own message");
            throw std::logic_error("logical error");
        }

        uint32_t INTER_IDX = package->INTER_IDX;
        seed_t SEED = package->SEED;
        // if (SEED > 10'000)
        //     stacktrace();
        Metadata META = package->META;
        // std::cout << "SMEKDA ALL ON THE FLOOR " << (int)META << std::endl;

        for (seed_t confirmed: package->SEEDS)
            log(std::format("confirms {}", fmt_seed(confirmed, INTER_IDX)));

        if (package->PAYLOAD.size()) {
            std::string maybe_fin = (META == Metadata::FINAL_SEGMENT ? " final" : "");
            log(std::format("is sending ${}'s{} segment ({}): {}; start={}", INTER_IDX, maybe_fin, fmt_seed(SEED, package->INTER_IDX), log_fmt(package->PAYLOAD), package->SEGMENT_START));
        } else if (META == Metadata::STOP_SENDING) {
            log(std::format("is sending \"please, stop\" {}", fmt_seed(SEED, INTER_IDX)));
        } else {
            log(std::format("is sending a segment <none> {}", fmt_seed(SEED, INTER_IDX)));
        }
    }

    py::object base;
    WiseProtocol(py::object base)
        : base(base)
        , pipe(base)
    {
        name = nr_nodes % 2 == 0 ? "\033[34;1mMr B\033[0m" : "\033[31;1mMr J\033[0m";
        nr_nodes += 1;
    }

    std::pair<int, Bytes> send_segment(Segment segment, std::vector<seed_t>& seeds_to_confirm)
    {
        auto [seed, message] = pipe.send_package(segment, seeds_to_confirm, inter_idx,
                               segment.is_final ? Metadata::FINAL_SEGMENT : Metadata::GENERAL);
        auto package = *PackageWrapper().feed(message);
        if (package.SEED != seed) {
            log("The hell is down");
            stacktrace();
        }
        log_sent_package(message);
        return std::make_pair(seed, message);
    }

    std::string role;
    unsigned long send(Bytes data)
    {
try {
        role = "send";
        inter_idx++;

        time_ms_t start_time = clock_ms();
        log(std::format("ready for transaction ${} as sender", inter_idx));

        int o_nr_hanging_segments = 10;
        int o_segment_len = 50000;

        enum {
            NEXT_SEGMENT = 1,
            CHECK_INCOMING = 2,
            RETRY_SEGMENT = 3,
            DONE = 4,
        };

        // chunks = [];
        // packages = [];
        std::vector<seed_t> seeds_to_confirm;
        std::map<int, Bytes> sent;
        int i = 0;

        int state = NEXT_SEGMENT;
        time_ms_t latest_retry = clock();

        std::vector<Package> next_inter;

        Guard_frequent time_guard(o_no_hear);
        Freq_remember guard;
        while (state != DONE) {
            // log(std::format("is in state {}", state));
            switch (state) {
            case NEXT_SEGMENT: {
                if (i == data.size()) {
                    state = sent.size() ? CHECK_INCOMING : DONE;
                    continue;
                }

                if (sent.size() == o_nr_hanging_segments) {
                    state = CHECK_INCOMING;
                    continue;
                }

                int start = i, end = i + o_segment_len;
                end = std::min(end, (int)data.size());
                Bytes sub(data.begin() + start, data.begin() + end);
                Segment segment(start, sub, end == data.size());

                auto [seed, message] = send_segment(segment, seeds_to_confirm);
                i = end;
                sent[seed] = message;
                break;
            }
            case CHECK_INCOMING: {
                if (time_guard.bad()) {
                    log("GOT FUCKING TIRED");
                    state = DONE;
                    break;
                }

                // log("will check for incoming");
                auto package = pipe.incoming(0.01);
                // log("got \'em, eat \'em");
                if (!package) {
                    state = RETRY_SEGMENT;
                    break;
                }

                time_guard.ping();
                log(std::format("received segment {}; {}", log_fmt(package->PAYLOAD), fmt_seed(package->SEED, package->INTER_IDX)));

                if (package->INTER_IDX < inter_idx) {
                    log(std::format("got a message from a weirdly old interaction {}", fmt_seed(package->SEED, package->INTER_IDX)));
                    state = NEXT_SEGMENT;
                    break;
                } else if (package->INTER_IDX > inter_idx) {
                    log("sees his partner has moved on");
                    next_inter.push_back(*package);
                    state = DONE;
                    break;
                } else if (package->META == Metadata::STOP_SENDING) {
                    log("has received a request to stop");
                    state = DONE;
                    break;
                }

                for (seed_t seed: package->SEEDS) {
                    log(std::format("considers confirmed {}", seed));
                    sent.erase(seed);
                }
                seeds_to_confirm.push_back(package->SEED);

                if (state == CHECK_INCOMING)
                    state = NEXT_SEGMENT;
                break;
            }
            case RETRY_SEGMENT: {
                if (guard.since_last_ping() < o_retry_ms) {
                    state = CHECK_INCOMING;
                    break;
                }
                guard.ping();

                if (sent.size() == 0)
                    throw std::logic_error("logic error");

                auto [seed, message] = *sent.begin();
                log(std::format("retries to send {}", fmt_seed(seed, inter_idx)));
                py_call(base, "sendto", py::bytes(message));
                break;
            }
            default:
                throw std::logic_error("invalid state");
            }
        }

        log(std::format("collected {} items for the next interaction", next_inter.size()));
        received_packages = next_inter;

        // log(std::format("send() in {:.5f} ms", double(clock_ms() - start_time)));

        return data.size();
} catch (std::logic_error e) {
    bool f = true;
    while (true) {
        if (f) {
            std::ofstream("black-bottle") << "CAUGHT AN EXCEPTION " + std::string(e.what()) << std::endl;
            log("CAUGHT AN EXCEPTION OF TYPE " + currentExceptionTypeName());
            f = false;
        }
    }
}
    }
    std::vector<Package> received_packages;

    py::bytes recv(int n)
    {
        role = "recv";
        inter_idx++;

        time_ms_t start_time = clock_ms();
        log(std::format("ready for transaction ${} as listener", inter_idx));

        Bytes buff(n, 0);

        std::vector<Package> received = std::move(received_packages);
        log(std::format("started listening with {} items packages", received.size()));

        std::set<seed_t> sent;
        std::set<unsigned long> seen;
        std::vector<seed_t> seeds_to_confirm;
        int nr_collected = 0;
        int nr_required = -1;

        enum {
            LISTEN = 1,
            PROCESS_RECEIVED = 2,
            CONFIRM = 3,
            ASK_TO_STOP = 4,
            DONE = 5,
        };

        int state = PROCESS_RECEIVED;

        Guard_frequent time_guard(o_no_hear);
        Freq_remember guard, confirm_guard;
        while (state != DONE) {
            // log(std::format("is in state {}", state));
            switch (state) {
            case LISTEN: {
                static int silence_counter = 0;
                silence_counter++;
                if (time_guard.bad()) {
                    log("GOT FUCKING TIRED");
                    state = DONE;
                    break;
                }

                auto package = pipe.incoming();
                if (package) {
                    std::string maybe_fin = (package->META == Metadata::FINAL_SEGMENT ? " final" : "");
                    log(std::format("received{} segment {}; {}", maybe_fin, log_fmt(package->PAYLOAD), fmt_seed(package->SEED, package->INTER_IDX)));
                    received.push_back(*package);
                    state = PROCESS_RECEIVED;
                    time_guard.ping();
                    silence_counter = 0;
                }

                if (state == LISTEN && silence_counter > 10) {
                    state = CONFIRM;
                }
                break;
            }
            case PROCESS_RECEIVED: {
                for (auto package: received) {
                    for (seed_t seed: package.SEEDS) {
                        log(std::format("considers confirmed {}", seed));
                        sent.erase(seed);
                    }

                    if (package.INTER_IDX < inter_idx) {
                        log(std::format("got a message from a weirdly old interaction {}", fmt_seed(package.SEED, package.INTER_IDX)));
                        state = ASK_TO_STOP;
                        continue;
                    }

                    seeds_to_confirm.push_back(package.SEED);

                    if (seen.count(package.SEGMENT_START)) {
                        continue;
                    }
                    seen.insert(package.SEGMENT_START);

                    auto i = package.SEGMENT_START;
                    auto s = package.PAYLOAD;
                    nr_collected += s.size();
                    if (package.META == Metadata::FINAL_SEGMENT) {
                        nr_required = i + s.size();
                    }
                    for (int j = 0; j < s.size(); j++)
                        buff[i + j] = s[j];

                    if (nr_collected == nr_required) {
                        log("COLLECTED THEM ALL");
                        state = DONE;
                    }
                }
                received.clear();
                if (state == PROCESS_RECEIVED) {
                    state = LISTEN;
                }
                break;
            }
            case CONFIRM: {
                if (confirm_guard.since_last_ping() < o_retry_ms) {
                    state = LISTEN;
                    break;
                }
                confirm_guard.ping();
                if (seeds_to_confirm.size() > 0) {
                    auto [seed, message] = pipe.send_package({}, seeds_to_confirm, inter_idx, Metadata::NONE);
                    log_sent_package(message);
                }
                state = LISTEN;
                break;
            }
            case ASK_TO_STOP: {
                if (guard.since_last_ping() < 200) {
                    state = LISTEN;
                    continue;
                }
                guard.ping();

                std::vector<seed_t> dummy;
                auto [seed, message] = pipe.send_package({}, dummy, inter_idx, Metadata::STOP_SENDING);
                log_sent_package(message);
                state = LISTEN;
                break;
            }
            default:
                throw std::logic_error("invalid state");
            }
        }
        // std::cout << "Cycle down" << std::endl;

        log(std::format("received ${}: {}\n", inter_idx, log_fmt(buff)));

        std::vector<seed_t> dummy;
        auto [seed, message] = pipe.send_package({}, dummy, inter_idx, Metadata::STOP_SENDING);
        log_sent_package(message);

        if (nr_required == -1 || nr_collected < nr_required) {
            log("RECEIVED INCOMPLETE DATA");
            while (false) {
                std::this_thread::sleep_for(std::chrono::milliseconds(950));
                log("has caught an exception");
            }
            throw std::logic_error("RECEIVED INCOMPLETE DATA");
        }

        // log(std::format("recv() in {:.5f} ms", (clock_ms() - start_time) / 1.));

        // std::cout << "Convertation about to be convoluted" << std::endl;
        py::bytes tmp(buff);
        // std::cout << "CONVERTATION CONVOLUTED" << std::endl;
        return tmp;
    }
};

int WiseProtocol::nr_nodes = 0;

PYBIND11_MODULE(wise_protocol, handle) {
    handle.doc() = "Very wise words";
    py::class_<WiseProtocol>(handle, "WiseProtocol")
        .def(py::init<py::object>())
        .def("send", &WiseProtocol::send)
        .def("recv", &WiseProtocol::recv);
}
