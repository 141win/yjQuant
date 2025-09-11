# yjQuant v3.0 - 量化交易系统核心组件

## 项目概述

yjQuant v3.0 是一个重新设计的量化交易系统核心架构，采用清晰的职责分离原则，构建了五个核心组件：

- **事件引擎 (EventEngine)**: 纯事件分发器
- **时钟引擎 (ClockEngine)**: 纯定时事件发布器  
- **配置管理器 (ConfigManager)**: 纯配置管理器
- **策略引擎 (StrategyEngine)**: 策略管理器
- **数据引擎 (DataEngine)**: 数据管理器

## 架构设计原则

### 1. 单一职责原则
每个组件只负责自己的核心功能，不越权处理其他职责。

### 2. 事件驱动架构
组件间通过事件机制进行解耦通信，时钟引擎到时间发布事件，事件引擎分发事件，具体业务逻辑由事件订阅者处理。

### 3. 配置驱动
系统行为完全由配置文件驱动，支持热重载和变更通知。

## 核心组件详解

### EventEngine (事件引擎)
- **职责**: 纯事件分发器，不定义事件类型，不关心事件内容
- **功能**: 
  - 维护事件订阅表 `{event_type: [handlers]}`
  - 接收事件并分发给所有订阅者
  - 处理订阅者的异常，不影响其他订阅者
- **接口**: `subscribe()`, `unsubscribe()`, `publish()`

### ClockEngine (时钟引擎)
- **职责**: 纯定时事件发布器，不执行具体任务逻辑
- **功能**:
  - 维护定时任务列表，按执行时间排序
  - 到达执行时间时向事件引擎发布事件
  - 支持任务的启用/禁用管理
- **接口**: `register_task()`, `unregister_task()`, `enable_task()`, `disable_task()`

### ConfigManager (配置管理器)
- **职责**: 纯配置管理器
- **功能**:
  - 加载和管理所有配置文件
  - 提供配置查询接口
  - 定时检查配置变更
  - 通过事件引擎通知配置变更
- **接口**: `get_config()`, `reload_config()`, `get_config_names()`

### StrategyEngine (策略引擎)
- **职责**: 策略管理器
- **功能**:
  - 管理所有策略类实例
  - 向时钟引擎注册策略检查任务
  - 向事件引擎订阅策略检查事件
  - 向事件引擎订阅数据到达事件，执行所有策略
- **接口**: `start()`, `stop()`, `get_strategy_count()`

### DataEngine (数据引擎)
- **职责**: 数据管理器
- **功能**:
  - 协调Redis管理器、数据源管理器和数据库管理器
  - 向时钟引擎注册定时"请求数据"任务
  - 向事件引擎订阅"数据请求"事件、"数据引擎配置变更"事件
  - 实现三级回退机制获取数据
  - 发布"数据到达"事件
- **接口**: `start()`, `stop()`, `get_status()`

## 配置文件结构

系统包含4个核心配置文件：

### 1. system.json - 系统配置
```json
{
    "name": "yjQuant v3.0",
    "version": "3.0.0",
    "debug": false,
    "log_level": "INFO"
}
```

### 2. email.json - 邮箱配置
```json
{
    "enabled": false,
    "smtp_server": "smtp.gmail.com",
    "smtp_port": 587
}
```

### 3. strategy.json - 策略配置
```json
{
    "enabled": true,
    "strategies": [...],
    "default_parameters": {...}
}
```

### 4. data_engine.json - 数据引擎配置
```json
{
    "redis": {...},
    "data_sources": [...],
    "data_fetch": {...},
    "cache": {...}
}
```

## 工作流程

### 启动流程
1. 创建事件引擎 → 启动事件引擎
2. 创建时钟引擎 → 启动时钟引擎
3. 创建配置管理器 → 启动配置管理器
4. 注册事件处理器
5. 注册定时任务

### 运行时流程
1. **定时任务执行**: 时钟引擎到时间 → 发布定时事件 → 事件引擎分发 → 订阅者处理
2. **配置变更检测**: 配置管理器检测变更 → 发布配置变更事件 → 事件引擎分发 → 订阅者处理

### 关闭流程
1. 停止配置管理器
2. 停止时钟引擎  
3. 停止事件引擎

## 使用方法

### 基本使用
```python
import asyncio
from core.event_engine import EventEngine
from core.clock_engine import ClockEngine
from core.config_manager import ConfigManager

async def main():
    # 创建组件
    event_engine = EventEngine()
    clock_engine = ClockEngine(event_engine)
    config_manager = ConfigManager("./config")
    
    # 启动组件
    await event_engine.start()
    await clock_engine.start(event_engine)
    await config_manager.start(event_engine, clock_engine)
    
    # 注册事件处理器
    event_engine.subscribe("heartbeat", lambda data: print(f"心跳: {data}"))
    
    # 注册定时任务
    clock_engine.register_task("heartbeat", "heartbeat", 10)
    
    # 运行
    await asyncio.sleep(60)

# 运行
asyncio.run(main())
```

### 运行示例
```bash
cd yjQuant_v3
python example.py
```

## 项目结构
```
yjQuant_v3/
├── core/                    # 核心组件
│   ├── __init__.py
│   ├── event_engine.py     # 事件引擎
│   ├── clock_engine.py     # 时钟引擎
│   ├── config_manager.py   # 配置管理器
│   ├── strategy_engine.py  # 策略引擎
│   ├── data_engine.py      # 数据引擎
│   ├── redis_manager.py    # Redis管理器
│   ├── data_source_manager.py # 数据源管理器
│   └── db_manager.py       # 数据库管理器
├── strategies/              # 策略模块
│   ├── __init__.py
│   ├── strategy_template.py # 策略模板基类
│   └── dip_rebound_strategy.py # 示例策略实现
├── config/                  # 配置文件
│   ├── __init__.py
│   ├── system.json         # 系统配置
│   ├── email.json          # 邮箱配置
│   ├── strategy.json       # 策略配置
│   └── data_engine.json    # 数据引擎配置
├── example.py              # 使用示例
├── test_strategy_system.py # 策略系统测试
├── test_data_engine.py     # 数据引擎测试
├── README.md               # 项目说明
└── __init__.py             # 包初始化
```

## 技术特性

- **异步架构**: 基于 asyncio 的高性能异步架构
- **类型提示**: 完整的 Python 类型提示支持
- **异常处理**: 完善的异常处理和日志记录
- **资源管理**: 优雅的启动和关闭流程
- **配置热重载**: 支持配置文件的动态重载
- **事件驱动**: 松耦合的事件驱动架构

## 扩展性

系统设计为高度可扩展的架构：

1. **新事件类型**: 直接发布新事件类型，无需修改事件引擎
2. **新定时任务**: 向时钟引擎注册新任务即可
3. **新配置**: 在配置目录添加新配置文件
4. **新组件**: 通过事件机制与现有组件集成

## 开发规范

- 遵循 PEP 8 代码规范
- 使用 Google 风格的文档字符串
- 完整的类型提示
- 异步编程最佳实践
- 完善的日志记录

## 许可证

本项目采用 MIT 许可证。
