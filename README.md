# Lsm Tree存储引擎
项目基于CMU迟老师的Mini Lsm项目衍生而来。实现了一个高并发，高可用，高安全的LsmTree存储引擎

## 环境配置
```bash
sudo apt-get install build-essential # build-essential packages, including gcc,g++, make and so on
sudo apt-get install cmake # cmake package
```
## 运行项目
```bash
git clone https://github.com/lucklyda/lsm_cpp.git
cd lsm_cpp
mkdir build
cd build
cmake ..
make
```
## 外部使用
程序提供对外接口，仅需将项目源码集成到外部项目，即可正常使用
```c++
#include "lsm_tree.h"
//create lsm tree
LsmTree tree(const char* table_name,LsmTreeOptions option);
//insert delete get
tree.get(std::string_view key);
tree.delete(std::string_view key);
tree.put(std::string_view key,Value value);
//get snapshot
auto txn = tree.new_txn();
//snapshot can also using put delete get
//commit your snapshot if check_rollback==false,means Transaction failed
bool check_rollback = txn.commit();
// if you want rollback you Transaction,juse release the txn
```