
#include "skipmap.h"
#include <iostream>
#include <thread>
#include <vector>
#include <cstdint>
#include <random>
#include <chrono>
#include <unordered_map>
#include <cassert>

struct LsmKey {
    std::string user_key;
    uint64_t ts;

    // 必须支持 operator< （跳表和 LSM 核心要求）
    bool operator<(const LsmKey& other) const {
        if (user_key != other.user_key) {
            return user_key < other.user_key;
        }
        // 同key：时间戳大的 < 时间戳小的 → 排在前面
        return ts > other.ts;
    }

    // 必须支持赋值（静态检查要求）
    LsmKey& operator=(const LsmKey& other) = default;
    bool operator==(const LsmKey& other) const {
        return user_key == other.user_key && ts == other.ts;
    }
    LsmKey(const LsmKey&) = default;
    LsmKey() = default;
};

// 方便打印
std::ostream& operator<<(std::ostream& os, const LsmKey& k) {
    os << "[key=" << k.user_key << ", ts=" << k.ts << "]";
    return os;
}

using u64 = uint64_t;
using namespace std;

// ---------------------------
// 1. 单线程大数据：写入立刻读取校验
// ---------------------------
void test_big_data_correctness() {
    cout << "\n=== 单线程 10w 数据：写入+实时读取校验 ===" << endl;
    SkipMap<LsmKey, u64> map;
    const int N = 100000;

    // 全局预期表，用来最后对账
    unordered_map<string, u64> expect;

    for (int i = 0; i < N; ++i) {
        LsmKey k{"data_" + to_string(i), 100};
        u64 val = i * 100ULL;

        map.put(k, val);
        expect[k.user_key] = val;

        // 立刻读取校验：必须一致
        u64 res = map.get(k);
        assert(res == val && "single thread read after write mismatch!");
    }

    // 全量遍历对账
    auto iter = map.create_iterator();
    int cnt = 0;
    while (iter->is_valid()) {
        auto key = iter->get_key();
        auto val = iter->get_value();
        assert(expect.count(key.user_key) && "iter find unexpected key");
        assert(expect[key.user_key] == val && "iter value mismatch");
        cnt++;
        iter->next();
    }
    delete iter;

    assert(cnt == N && "total count mismatch");
    cout << "✅ 10w 单线程：写入+实时校验+遍历对账 全部正确\n";
}

// ---------------------------
// 2. 多线程并发写入 + 全局对账校验
// ---------------------------
// 线程共享：预期map + 保护锁
unordered_map<string, u64> global_expect;
mutex global_mtx;

void thread_write_task(SkipMap<LsmKey, u64>& map, int tid, int per_thread_cnt) {
    for (int i = 0; i < per_thread_cnt; ++i) {
        string uk = "thr" + to_string(tid) + "_k" + to_string(i);
        LsmKey key{uk, 200ULL};
        u64 value = (u64)tid * 100000 + i;

        // 1. 写入跳表
        map.put(key, value);

        // 2. 本线程立刻读取校验
        u64 res = map.get(key);
        assert(res == value && "multi-thread immediate read wrong!");

        // 3. 写入全局预期表
        lock_guard<mutex> lk(global_mtx);
        global_expect[uk] = value;
    }
}

void test_multi_thread_correctness() {
    cout << "\n=== 多线程并发写入 + 全局对账校验 ===" << endl;
    SkipMap<LsmKey, u64> map;

    const int thread_num = 4;
    const int per_cnt = 25000;
    global_expect.clear();

    vector<thread> ths;
    for (int t = 0; t < thread_num; t++) {
        ths.emplace_back(thread_write_task, ref(map), t, per_cnt);
    }
    for (auto& th : ths) th.join();

    // 遍历全盘对账
    auto iter = map.create_iterator();
    int actual_count = 0;
    while (iter->is_valid()) {
        auto k = iter->get_key();
        auto v = iter->get_value();

        lock_guard<mutex> lk(global_mtx);
        assert(global_expect.count(k.user_key) && "global expect missing key");
        assert(global_expect[k.user_key] == v && "global value mismatch");
        actual_count ++;
        iter->next();
    }
    delete iter;

    int expect_total = thread_num * per_cnt;
    assert(actual_count == expect_total && "multi thread total count wrong");
    cout << "✅ 多线程并发：实时校验 + 全局对账 全部正确\n";
}

// ---------------------------
// 3. 多线程读写混合 + 容错校验
// ---------------------------
void read_only_task(SkipMap<LsmKey, u64>& map, int tid, int rounds) {
    for (int i = 0; i < rounds; ++i) {
        LsmKey k{"mix_rd_" + to_string(tid) + "_" + to_string(i), 300ULL};
        try {
            map.get(k);
        } catch (...) {
            // 读不到是正常的，不报错
        }
    }
}

void write_mix_task(SkipMap<LsmKey, u64>& map, int tid, int rounds) {
    for (int i = 0; i < rounds; ++i) {
        string uk = "mix_wr_" + to_string(tid) + "_" + to_string(i);
        LsmKey key{uk, 300ULL};
        u64 val = tid * 50000 + i;

        map.put(key, val);
        assert(map.get(key) == val && "mix write read check fail");
    }
}

void test_mixed_rw_correctness() {
    cout << "\n=== 多线程混合读写正确性测试 ===" << endl;
    SkipMap<LsmKey, u64> map;

    int round = 10000;
    thread r1(read_only_task, ref(map), 1, round);
    thread r2(read_only_task, ref(map), 2, round);
    thread w1(write_mix_task, ref(map), 1, round);
    thread w2(write_mix_task, ref(map), 2, round);

    r1.join(); r2.join();
    w1.join(); w2.join();

    cout << "✅ 混合读写无崩溃、无断言错误，安全正确\n";
}

int main() {
    ios::sync_with_stdio(false);
    cin.tie(nullptr);

    test_big_data_correctness();
    test_multi_thread_correctness();
    test_mixed_rw_correctness();

    cout << "\n===== ALL CORRECT TEST PASSED =====\n";
    return 0;
}