# bbolt++ 测试文件

这个目录包含了bbolt++项目的所有测试文件。

## 测试文件结构

### 基础测试
- `test_basic.cpp` - 基础功能测试
- `test_simple.cpp` - 简单功能测试

### 核心功能测试
- `test_db.cpp` - 数据库核心功能测试
- `test_bucket.cpp` - 桶操作测试

### 详细测试套件
- `db_test_simple.cpp` - 数据库详细测试（使用自定义测试框架）
- `bucket_test_simple.cpp` - 桶操作详细测试（使用自定义测试框架）

### 扩展测试（基于gtest）
- `db_test.cpp` - 数据库gtest测试套件
- `bucket_test.cpp` - 桶操作gtest测试套件
- `tx_test.cpp` - 事务gtest测试套件
- `cursor_test.cpp` - 游标gtest测试套件

### 测试框架
- `test_framework.h` - 自定义测试框架头文件

## 运行测试

### 使用xmake编译
```bash
xmake
```

### 运行单个测试
```bash
./build/macosx/arm64/debug/test_basic
./build/macosx/arm64/debug/test_simple
./build/macosx/arm64/debug/test_db
./build/macosx/arm64/debug/test_bucket
./build/macosx/arm64/debug/test_db_simple
./build/macosx/arm64/debug/test_bucket_simple
```

### 运行所有测试
```bash
./run_tests.sh
```

## 测试状态

- ✅ 基础功能测试 - 通过
- ✅ 数据库打开/关闭 - 通过
- ⚠️ 桶操作测试 - 部分通过（存在cursor问题）
- ⚠️ 详细测试套件 - 部分通过（存在cursor问题）

## 已知问题

1. **Checksum验证问题** - meta页面的checksum计算不正确，目前临时禁用了验证
2. **Cursor无限循环** - 在bucket测试中，cursor操作可能导致无限循环
3. **Gtest依赖** - 部分测试需要gtest库，目前使用自定义测试框架

## 开发说明

- 所有测试文件都使用相对路径引用项目头文件
- 测试框架提供了基本的断言宏和测试套件管理
- 测试文件按功能模块组织，便于维护和扩展
