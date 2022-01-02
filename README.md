# spadgerdb

> "麻雀虽小，五脏俱全。"

## 简介

spadgerdb 是一个简易的日志型key/value存储引擎，参考[leveldb](https://github.com/google/leveldb )的设计（因为时间关系，没有实现SSTable），能够嵌入到其他python程序中，支持四种基本操作:

- get
- put
- delete
- write (batch)

## 为什么要做这个项目

一直以来对存储引擎很感兴趣，但是一直没有合适的实践机会。得益于本学期的[SJTU-CS902-3（程序设计思想与方法）](https://oc.sjtu.edu.cn/courses/33672 )，让我有足够的动力去做一个玩具级别的存储引擎。为了完成这个项目，我有目的地阅读了leveldb源码，学习了存储引擎的一些实现细节。

## 实验环境

python 版本 >= 3.9.9

## 项目结构

源文件放在根目录下，一些重要源文件的说明：

- db.py: 接口层，提供用户get、put、delete和write的接口
- version_set.py: 版本控制，实现了版本链（很遗憾，因为没有实现compaction，没有充分用到）
- version_edit.py: 版本控制，实现版本的变动记录
- skiplist.py: 快表，内存数据库的底层实现
- memtable.py: 内存数据库，基于skiplist，利用编解码，提供快照读
- bloom_filter.py: bloom过滤器，用于提高查询效率（很遗憾没有用到）
- cli.py: 一个命令行客户端，方便地操作数据库

单元测试放在test目录下：

可通过python内置单元测试框架unittest来运行单元测试，可以看到测试结果，例如：

```bash
python -m unittest test.test_db
```

为清除临时文件，可运行：

```bash
make clean
```

## 使用

### 初始化
```python
from db import DB
from option import DBOption
option = DBOption()
option.create_if_missing = True # The option argument will lead to create a new DB if it does not exist.
db, s = DB.open(option)
if not s.ok():
    # Do something to handle error
    pass
```

### 修改

```python
from option import WriteOption
from write_batch import WriteBatch

# use single write
db.put(WriteOption(), "key1", "value1")
db.delete(WriteOption(), "key1")

# use write batch
batch = WriteBatch()
batch.put("key2", "value2")
batch.put("key3", "value3")
db.write(WriteOption(), batch)
```

### 读取

```python
from option import ReadOption
from status import Status

# Read lastest version
option1 = ReadOption()
value = []
status = db.get(option1, "key1", value)
assert status.ok() or status == Status.NotFound()

# Read specific version
option2 = ReadOption()
value = []
status = db.get(option2, "key1", value)
assert status.ok() or status == Status.NotFound()
```

### 关闭

```python
db.close()
```

## 客户端

本项目提供了命令行客户端，可运行以下命令打开：

```bash
$ python cli.py
```

提示输入数据库名称，然后进入交互式界面：

```txt
Enter database name:
>>> my_db
> 
```

### 基本使用

```txt
> get name                            # get命令，获取key对应的值，可一次性获取多个，未找到或者被删除的值输出空行

> put name qiuhuiming id 518021911027 # put命令，可指定多个kv对，各个kv对相邻
> get name id
qiuhuiming
518021911027
> del name                            # delete命令，删除key
> del id
> get name id


> exit                                # 退出
bye~
```

### 快照读

```txt
> put key1 value1 key2 value2
> seq                                # 查看当前版本号
2
> put key1 value3 key2 value4
> seq
4
> get_2 key1                         # get_<version_sequence> 查看指定版本
value1
> get_2 key2
value2
> get_4 key1
value3
> exit
bye~

```

### 日志恢复

```txt
$ python cli.py
Enter database name:
>>> test_recover
> put name qiuhuiming
> get name
qiuhuiming
> exit
bye~
$ python cli.py                       # Recover the database.
Enter database name:
>>> test_recover
> get name
qiuhuiming


```

