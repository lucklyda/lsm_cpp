#include <iostream>
#include <vector>
#include <string>
#include <chrono>
#include <random>
#include <thread>
#include <atomic>
#include <filesystem>
#include "lsm_tree.h"

// 类型别名适配你的代码
using usize = size_t;
namespace fs = std::filesystem;

// ======================  Benchmark 工具函数 ======================
// 生成随机字符串（键/值）
std::string random_string(size_t length) {
    static const char charset[] = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz";
    static std::mt19937 rng(std::random_device{}());
    std::uniform_int_distribution<> dist(0, sizeof(charset) - 2);
    std::string str;
    str.reserve(length);
    for (size_t i = 0; i < length; ++i) {
        str += charset[dist(rng)];
    }
    return str;
}

// 清空测试目录
void clean_test_dir(const std::string& path) {
    if (fs::exists(path)) {
        fs::remove_all(path);
    }
    fs::create_directories(path);
}

// 计时工具
using TimePoint = std::chrono::high_resolution_clock::time_point;
inline TimePoint now() {
    return std::chrono::high_resolution_clock::now();
}

// 计算耗时（毫秒）
inline double elapsed_ms(TimePoint start, TimePoint end) {
    return std::chrono::duration<double, std::milli>(end - start).count();
}

// 打印测试结果
void print_result(const std::string& name, size_t ops, double ms) {
    double qps = ops / (ms / 1000.0);
    printf("[%s]\n  操作数: %zu | 耗时: %.2f ms | QPS: %.2f\n\n",
           name.c_str(), ops, ms, qps);
}

// ======================  LSM-Tree 测试配置 ======================
const std::string TEST_DIR = "./lsm_benchmark_data";
const size_t TEST_KEY_COUNT = 100000;     // 基础测试键值对数量
const size_t THREAD_COUNT = 8;            // 多线程测试线程数
const size_t LARGE_VALUE_SIZE = 1024;     // 大值大小 1KB
const size_t SMALL_VALUE_SIZE = 16;       // 小值大小 16B

// 初始化你的 LSM-Tree 配置
LsmStorageOptions default_options() {
    LsmStorageOptions options{};
    options.block_size = 4096;            // 4KB 块大小
    options.target_sst_size = 1 * 1024 * 1024; // 16MB SST
    options.num_memtable_limit = 3;       // 内存表数量限制
    options.enable_wal = true;            // 开启WAL
    options.serializable = true;          // 可串行化事务
    auto temp = LeveledCompactionOptions();
    auto temp1 = NoCompactionOptions();
    
    temp.base_level_size_mb = 4;
    temp.level0_file_num_compaction_trigger=3;
    temp.level_size_multiplier = 2;
    temp.max_levels = 5;

    options.compaction_options = std::make_shared<LeveledCompactionOptions>(temp); // 层级压缩
    
    return options;
}

// ======================  数据库内核全场景 Benchmark ======================
/**
 * 测试1：单线程 随机写入（小键值）
 * 场景：数据库基础写入性能、MemTable 切换、Flush 性能
 */
void bench_single_write_random() {
    clean_test_dir(TEST_DIR);
    LsmTree lsm(TEST_DIR, default_options());
    
    auto start = now();
    for (size_t i = 0; i < TEST_KEY_COUNT; ++i) {
        std::string key = "key_" + random_string(16);
        std::string val = random_string(SMALL_VALUE_SIZE);
        lsm.put(key, val.c_str());
    }
    auto end = now();

    print_result("单线程随机写入(小键值)", TEST_KEY_COUNT, elapsed_ms(start, end));
}

/**
 * 测试2：单线程 顺序写入（小键值）
 * 场景：数据库批量导入、顺序写入优化
 */
void bench_single_write_sequential() {
    clean_test_dir(TEST_DIR);
    LsmTree lsm(TEST_DIR, default_options());

    auto start = now();
    for (size_t i = 0; i < TEST_KEY_COUNT; ++i) {
        std::string key = "key_" + std::to_string(i);
        std::string val = random_string(SMALL_VALUE_SIZE);
        lsm.put(key, val.c_str());
    }
    auto end = now();

    print_result("单线程顺序写入(小键值)", TEST_KEY_COUNT, elapsed_ms(start, end));
}

/**
 * 测试3：单线程 随机读取
 * 场景：数据库点查性能、缓存命中率、SST 查找性能
 */
void bench_single_read_random() {
    clean_test_dir(TEST_DIR);
    LsmTree lsm(TEST_DIR, default_options());

    // 先写入数据
    std::unordered_map<std::string,std::string> keys;
    for (size_t i = 0; i < TEST_KEY_COUNT; ++i) {
        std::string key = "key_" + random_string(16);
        std::string v = random_string(SMALL_VALUE_SIZE);
        Value vvv(v);
        keys[key]=v;
        lsm.put(key, vvv);
    }
    lsm.sync(); // 确保数据落盘

    // 随机读取
    auto start = now();
    size_t hit = 0;
    size_t null_val=0;
    for (const auto& key : keys) {
        auto val = lsm.get(key.first);
        if(val.is_empty()){
            null_val++;
        }else if(key.second==val.value){
            hit++;
        }
    }
    std::cout<<"Null Value "<<null_val<<std::endl;
    auto end = now();

    print_result("单线程随机读取(命中率100%)", TEST_KEY_COUNT, elapsed_ms(start, end));
    printf("  读取命中: %zu/%zu\n\n", hit, TEST_KEY_COUNT);
}

/**
 * 测试4：多线程 并发写入
 * 场景：数据库高并发写入、线程安全、锁竞争、Flush/Compaction 并发性能
 */
void bench_multi_write() {
    clean_test_dir(TEST_DIR);
    LsmTree lsm(TEST_DIR, default_options());

    std::atomic<size_t> total_ops = 0;
    size_t ops_per_thread = TEST_KEY_COUNT / THREAD_COUNT;

    auto start = now();
    std::vector<std::thread> threads;
    for (size_t t = 0; t < THREAD_COUNT; ++t) {
        threads.emplace_back([&]() {
            for (size_t i = 0; i < ops_per_thread; ++i) {
                std::string key = "thread_key_" + random_string(16);
                lsm.put(key, random_string(SMALL_VALUE_SIZE).c_str());
                total_ops++;
            }
        });
    }
    for (auto& t : threads) t.join();
    auto end = now();

    print_result("多线程并发写入(" + std::to_string(THREAD_COUNT) + "线程)",
                 total_ops.load(), elapsed_ms(start, end));
}

/**
 * 测试5：多线程 读写混合
 * 场景：数据库核心业务场景（写入+查询同时进行）
 */
void bench_multi_read_write() {
    clean_test_dir(TEST_DIR);
    LsmTree lsm(TEST_DIR, default_options());

    // 预写入基础数据
    for (size_t i = 0; i < TEST_KEY_COUNT / 2; ++i) {
        lsm.put("key_" + std::to_string(i), random_string(SMALL_VALUE_SIZE).c_str());
    }

    std::atomic<size_t> write_ops = 0, read_ops = 0;
    auto start = now();

    // 写线程
    std::thread writer([&]() {
        for (size_t i = 0; i < TEST_KEY_COUNT / 2; ++i) {
            lsm.put("new_key_" + random_string(16), random_string(SMALL_VALUE_SIZE).c_str());
            write_ops++;
        }
    });

    // 读线程
    std::vector<std::thread> readers;
    for (size_t t = 0; t < THREAD_COUNT - 1; ++t) {
        readers.emplace_back([&]() {
            for (size_t i = 0; i < TEST_KEY_COUNT / 2; ++i) {
                lsm.get("key_" + std::to_string(rand() % (TEST_KEY_COUNT / 2)));
                read_ops++;
            }
        });
    }

    writer.join();
    for (auto& t : readers) t.join();
    auto end = now();

    size_t total = write_ops + read_ops;
    printf("[多线程读写混合(写:%zu,读:%zu)]\n  总操作: %zu | 耗时: %.2f ms | 总QPS: %.2f\n\n",
           write_ops.load(), read_ops.load(), total, elapsed_ms(start, end),
           total / (elapsed_ms(start, end) / 1000.0));
}

/**
 * 测试6：MVCC 事务性能
 * 场景：数据库事务隔离、快照读、提交/回滚性能
 */
void bench_txn_mvcc() {
    clean_test_dir(TEST_DIR);
    LsmTree lsm(TEST_DIR, default_options());

    auto start = now();
    // 事务写入（提交）
    for (size_t i = 0; i < TEST_KEY_COUNT / 10; ++i) {
        auto txn = lsm.new_txn();
        lsm.put("txn_key_" + std::to_string(i), random_string(SMALL_VALUE_SIZE).c_str());
        txn->commit(); // 提交事务
    }

    // 事务回滚测试
    for (size_t i = 0; i < 1000; ++i) {
        auto txn = lsm.new_txn();
        lsm.put("rollback_key_" + std::to_string(i), "val");
        // 不commit = 自动回滚
    }
    auto end = now();

    print_result("MVCC事务(提交+回滚)", TEST_KEY_COUNT / 10 + 1000, elapsed_ms(start, end));
}

/**
 * 测试7：大键值写入/读取
 * 场景：数据库存储大对象、IO性能、块压缩性能
 */
void bench_large_kv() {
    clean_test_dir(TEST_DIR);
    LsmTree lsm(TEST_DIR, default_options());
    const size_t LARGE_KV_CNT = 1000; // 1000个1KB值

    auto start = now();
    for (size_t i = 0; i < LARGE_KV_CNT; ++i) {
        lsm.put("large_key_" + std::to_string(i), random_string(LARGE_VALUE_SIZE).c_str());
    }
    lsm.sync();
    double write_ms = elapsed_ms(start, now());

    start = now();
    for (size_t i = 0; i < LARGE_KV_CNT; ++i) {
        lsm.get("large_key_" + std::to_string(i));
    }
    double read_ms = elapsed_ms(start, now());

    printf("[大键值测试(1KB值)]\n  写入: %zu 耗时%.2fms | 读取: %zu 耗时%.2fms\n\n",
           LARGE_KV_CNT, write_ms, LARGE_KV_CNT, read_ms);
}

/**
 * 测试8：范围扫描性能
 * 场景：数据库范围查询、迭代器性能、SST 合并扫描性能
 */
void bench_scan() {
    clean_test_dir(TEST_DIR);
    LsmTree lsm(TEST_DIR, default_options());

    // 写入有序数据
    for (size_t i = 0; i < TEST_KEY_COUNT; ++i) {
        lsm.put("scan_key_" + std::to_string(i), "val");
    }
    lsm.sync();

    auto start = now();
    // 全量扫描
    Bound<std::string_view> lower;
    lower.type = 1;
    lower.key = "scan_key_0";
    Bound<std::string_view> upper;
    upper.type = 1;
    upper.key = "scan_key_" + std::to_string(TEST_KEY_COUNT);
    auto iter = lsm.scan(lower, upper);

    size_t count = 0;
    while (iter->is_valid()) {
        if(iter->value().value == "val"){
            count++;
        }
        iter->next();
        
    }
    auto end = now();

    print_result("范围扫描(全量" + std::to_string(TEST_KEY_COUNT) + "条)", count, elapsed_ms(start, end));
}

/**
 * 测试9：删除性能
 * 场景：数据库删除、墓碑值处理、Compaction 清理性能
 */
void bench_delete() {
    clean_test_dir(TEST_DIR);
    LsmTree lsm(TEST_DIR, default_options());

    // 先写入数据
    std::vector<std::string> keys;
    for (size_t i = 0; i < TEST_KEY_COUNT; ++i) {
        std::string key = "del_key_" + std::to_string(i);
        keys.push_back(key);
        lsm.put(key, "val");
    }

    auto start = now();
    for (const auto& key : keys) {
        lsm.delete_(key);
    }
    auto end = now();

    print_result("单线程随机删除", TEST_KEY_COUNT, elapsed_ms(start, end));
}

/**
 * 测试10：WAL + 故障恢复性能
 * 场景：数据库崩溃恢复、WAL 写入、重启加载速度
 */
void bench_crash_recovery() {
    clean_test_dir(TEST_DIR);
    const size_t RECOVERY_CNT = 50000;

    // 第一步：写入数据后直接退出（模拟崩溃）
    {
        LsmTree lsm(TEST_DIR, default_options());
        for (size_t i = 0; i < RECOVERY_CNT; ++i) {
            lsm.put("crash_key_" + std::to_string(i), "val");
        }
        // 不手动 sync，依赖 WAL 保证持久化
    }

    // 第二步：重新打开，测试恢复速度
    auto start = now();
    LsmTree lsm_recover(TEST_DIR, default_options());
    auto end = now();

    // 验证数据完整性
    size_t hit = 0;
    for (size_t i = 0; i < RECOVERY_CNT; ++i) {
        if (!lsm_recover.get("crash_key_" + std::to_string(i)).is_empty()) hit++;
    }

    printf("[崩溃恢复测试]\n  恢复耗时: %.2f ms | 恢复数据数: %zu/%zu (完整性)\n\n",
           elapsed_ms(start, end), hit, RECOVERY_CNT);
}

/**
 * 测试11：Compaction 后台性能
 * 场景：数据库压缩、磁盘空间回收、写入稳定性能
 */
void bench_compaction() {
    clean_test_dir(TEST_DIR);
    LsmTree lsm(TEST_DIR, default_options());

    // 大量写入触发多次 MemTable Flush 和 Compaction
    auto start = now();
    for (size_t i = 0; i < TEST_KEY_COUNT * 5; ++i) {
        lsm.put("compact_key_" + std::to_string(i), random_string(SMALL_VALUE_SIZE).c_str());
    }
    lsm.sync();
    std::this_thread::sleep_for(std::chrono::seconds(2)); // 等待后台压缩完成
    auto end = now();

    print_result("压缩压力测试(50万写入)", TEST_KEY_COUNT * 5, elapsed_ms(start, end));
}

// ======================  主函数：运行所有 Benchmark ======================
int main() {
    printf("==================== LSM-Tree 数据库内核 Benchmark ====================\n");
    printf("测试目录: %s | 线程数: %zu | WAL: 开启 | 事务: 可串行化\n\n",
           TEST_DIR.c_str(), THREAD_COUNT);

    // 运行全量测试
    bench_single_write_random();
    bench_single_write_sequential();
   bench_single_read_random();
    bench_multi_write();
    bench_multi_read_write();
    bench_txn_mvcc();
    bench_large_kv();
     bench_scan();
     bench_delete();
   bench_crash_recovery();
   bench_compaction();

    printf("=======================================================================\n");
    return 0;
}