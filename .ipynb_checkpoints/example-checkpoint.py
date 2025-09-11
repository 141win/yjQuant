# _*_ coding: UTF-8 _*_
"""
新邮件模块使用示例
演示如何使用重构后的邮件发送系统

主要功能:
- 基本使用示例
- 配置方案示例
- 系统监控示例
- 错误处理示例
- 模拟生产者程序
"""

import time
import uuid
import threading

from email import ConfigProfiles
from email import EmailTask, EmailMessage
from email import EmailSystemManager
from email import RedisManager, RedisManagerConfig, RedisConfig, QueueConfig


def example_producer_consumer():
    """模拟生产者-消费者示例"""
    print("🚀 模拟生产者-消费者示例")
    print("=" * 50)

    try:
        # 1. 创建消费者配置
        consumer_config = ConfigProfiles.qq_email_consumer(
            consumer_id="demo_consumer_001",
            username="1410959463@qq.com",
            password="pialgdtyxptuiiad",
            display_name="游元杰"
        )

        # 2. 创建Redis管理器配置（用于生产者）
        redis_config = RedisManagerConfig(
            redis=RedisConfig(
                host="47.108.199.234",
                port=6379,
                max_connections=5
            ),
            queue=QueueConfig(
                timeout=2.0
            )
        )

        # 3. 创建Redis管理器实例（生产者使用）
        redis_manager = RedisManager(redis_config)

        # 4. 创建系统管理器（消费者使用）
        system_manager = EmailSystemManager([consumer_config])

        # 5. 启动邮件系统（消费者）
        print("✅ 启动邮件消费者系统...")
        system_manager.start_system()

        # 6. 模拟生产者创建邮件任务
        print("📧 开始创建邮件任务...")
        email_message_list = [EmailMessage(subject=f"测试 #{1}",
                                           recipients=["13086397065@163.com"],
                                           content=f"这是第 {1} 封测试邮件，不必回复，发送时间: {time.strftime('%H:%M:%S')}",
                                           content_type="plain"
                                           ), EmailMessage(subject=f"gupiao #{2}",
                                                           recipients=["13086397065@163.com"],
                                                           content=f"这是第 {2} 封测试邮件，涨跌大于5%，请关注，发送时间: {time.strftime('%H:%M:%S')}",
                                                           content_type="plain"
                                                           ), EmailMessage(subject=f"gupiao #{3}",
                                                                           recipients=["13086397065@163.com"],
                                                                           content=f"这是第 {3} 封测试邮件，股票大跌，请关注，发送时间: {time.strftime('%H:%M:%S')}",
                                                                           content_type="plain"
                                                                           ), EmailMessage(subject=f"xunihuobi #{4}",
                                                                                           recipients=[
                                                                                               "13086397065@163.com"],
                                                                                           content=f"这是第 {4} 封测试邮件，连涨3天后，开始下跌，请关注，发送时间: {time.strftime('%H:%M:%S')}",
                                                                                           content_type="plain"
                                                                                           ),
                              EmailMessage(subject=f"天气 #{5}",
                                           recipients=["13086397065@163.com"],
                                           content=f"这是第 {5} 封测试邮件，今天开始降温，请及时添衣，发送时间: {time.strftime('%H:%M:%S')}",
                                           content_type="plain"
                                           ),
                              ]

        def producer_task():
            """生产者任务：创建邮件任务并插入队列"""
            try:
                for i in range(5):  # 创建5个邮件任务
                    # 创建邮件消息
                    # email_message = EmailMessage(
                    #     subject=f"测试邮件 #{i+1}",
                    #     recipients=["13086397065@163.com"],
                    #     content=f"这是第 {i+1} 封测试邮件，发送时间: {time.strftime('%H:%M:%S')}",
                    #     content_type="plain"
                    # )
                    #
                    # 创建邮件任务
                    task = EmailTask(
                        task_id=f"task_{i + 1:03d}_{int(time.time())}",
                        message=email_message_list[i]
                    )

                    # 插入Redis队列
                    if redis_manager.enqueue_task(task):
                        print(f"✅ 任务 {task.task_id} 已插入队列")
                        logger.info(f"生产者: 任务 {task.task_id} 已插入队列")
                    else:
                        print(f"❌ 任务 {task.task_id} 插入队列失败")
                        logger.error(f"生产者: 任务 {task.task_id} 插入队列失败")

                    # 控制生产速度，增加间隔避免SMTP频率限制
                    time.sleep(5)  # 每5秒创建一个任务，避免触发频率限制

                print("🎯 生产者任务完成，已创建5个邮件任务")

            except Exception as e:
                print(f"❌ 生产者任务异常: {e}")
                logger.error(f"生产者任务异常: {e}")

        # 7. 启动生产者线程
        producer_thread = threading.Thread(target=producer_task, name="Producer_Thread")
        producer_thread.daemon = True
        producer_thread.start()

        # 8. 监控系统状态
        print("📊 监控系统状态...")
        monitor_count = 0

        while monitor_count < 20:  # 监控20次
            try:
                # 获取系统状态
                status = system_manager.get_system_status()

                # 获取队列统计
                queue_stats = redis_manager.get_queue_stats()

                print(f"\n--- 监控 #{monitor_count + 1} ---")
                print(f"系统运行状态: {'运行中' if status['system_running'] else '已停止'}")
                print(f"运行时间: {status['uptime']:.1f}秒")
                print(f"消费者数量: {status['consumer_count']}")

                if queue_stats:
                    print(f"队列统计:")
                    print(f"  待处理: {queue_stats.pending_count}")
                    print(f"  成功: {queue_stats.success_count}")
                    print(f"  失败: {queue_stats.failed_count}")
                    print(f"  总计: {queue_stats.total_count}")

                # 获取消费者统计
                if status['consumer_stats']:
                    consumer = status['consumer_stats'][0]
                    print(f"消费者统计:")
                    print(f"  线程名: {consumer['thread_name']}")
                    print(f"  运行状态: {'运行中' if consumer['is_running'] else '已停止'}")
                    print(f"  已处理: {consumer['total_processed']}")
                    print(f"  成功: {consumer['total_success']}")
                    print(f"  失败: {consumer['total_failed']}")

                monitor_count += 1
                time.sleep(3)  # 每3秒监控一次

            except Exception as e:
                print(f"❌ 监控异常: {e}")
                logger.error(f"监控异常: {e}")
                break

        # 9. 等待生产者完成
        producer_thread.join(timeout=30)

        # 10. 获取最终状态
        print("\n" + "=" * 50)
        print("📊 最终系统状态:")

        final_status = system_manager.get_system_status()
        final_queue_stats = redis_manager.get_queue_stats()

        print(f"系统运行状态: {'运行中' if final_status['system_running'] else '已停止'}")
        print(f"总运行时间: {final_status['uptime']:.1f}秒")

        if final_queue_stats:
            print(f"最终队列统计:")
            print(f"  待处理: {final_queue_stats.pending_count}")
            print(f"  成功: {final_queue_stats.success_count}")
            print(f"  失败: {final_queue_stats.failed_count}")
            print(f"  总计: {final_queue_stats.total_count}")

        # 11. 停止系统
        print("\n🛑 停止邮件系统...")
        system_manager.stop_system()

        # 12. 清理Redis队列（可选）
        print("🧹 清理Redis队列...")
        redis_manager.clear_queue("email:pending")
        redis_manager.clear_queue("email:success")
        redis_manager.clear_queue("email:failed")

        print("✅ 模拟生产者-消费者示例完成！")

    except Exception as e:
        print(f"❌ 模拟生产者-消费者示例失败: {e}")
        logger.error(f"模拟生产者-消费者示例失败: {e}")

        # 确保系统被停止
        try:
            if 'system_manager' in locals():
                pass
                # system_manager.stop_system()
        except:
            pass


def example_basic_usage():
    """基本使用示例"""
    print("🚀 基本使用示例")
    print("=" * 50)

    try:
        # 1. 创建消费者配置
        config = ConfigProfiles.qq_email_consumer(
            consumer_id="test_consumer_001",
            username="1410959463@qq.com",
            password="pialgdtyxptuiiad",
            display_name="测试用户"
        )

        # 2. 创建系统管理器
        system_manager = EmailSystemManager([config])

        # 3. 启动系统
        with system_manager:
            print("✅ 邮件系统启动成功")

            # 4. 获取系统状态
            status = system_manager.get_system_status()
            print(f"系统状态: {status}")

            # 5. 运行一段时间
            print("系统运行中...")
            time.sleep(5)

            # 6. 获取最终状态
            final_status = system_manager.get_system_status()
            print(f"最终状态: {final_status}")

    except Exception as e:
        print(f"❌ 基本使用示例失败: {e}")


def example_custom_config():
    """自定义配置示例"""
    print("\n🔧 自定义配置示例")
    print("=" * 50)

    try:
        # 1. 创建自定义配置
        from email import SMTPConfig, RedisConfig, QueueConfig, EmailSenderConfig, RedisManagerConfig, \
            ConsumerConfig

        custom_config = ConsumerConfig(
            consumer_id="custom_consumer_001",
            thread_name="Custom_Consumer_001",
            email_sender=EmailSenderConfig(
                smtp=SMTPConfig(
                    host="smtp.company.com",
                    port=587,
                    username="system@company.com",
                    password="company_password",
                    use_ssl=False,
                    use_tls=True,
                    display_name="企业系统"
                ),
                connection_timeout=600,
                send_timeout=120,
                max_connection_failures=3
            ),
            redis_manager=RedisManagerConfig(
                redis=RedisConfig(
                    host="redis.company.com",
                    port=6379,
                    password="redis_password",
                    max_connections=20
                ),
                queue=QueueConfig(
                    timeout=2.0
                )
            ),
            send_interval=1.0,
            max_concurrent=3
        )

        # 2. 验证配置
        try:
            custom_config.__post_init__()
            print("✅ 自定义配置验证通过")
            print(f"SMTP服务器: {custom_config.email_sender.smtp.host}:{custom_config.email_sender.smtp.port}")
            print(f"Redis服务器: {custom_config.redis_manager.redis.host}:{custom_config.redis_manager.redis.port}")
            print(f"发送间隔: {custom_config.send_interval}秒")
        except ValueError as e:
            print(f"❌ 自定义配置验证失败: {e}")

    except Exception as e:
        print(f"❌ 自定义配置示例失败: {e}")


def example_quick_start():
    """快速启动示例"""
    print("\n⚡ 快速启动示例")
    print("=" * 50)

    try:
        # 使用便捷函数快速启动系统
        print("使用QQ邮箱配置快速启动邮件系统...")
        print("注意：请修改为实际的邮箱配置")

        # 这里只是演示，实际使用时需要提供真实的邮箱信息
        # run_email_system(
        #     profile_name="qq_email",
        #     username="your_email@qq.com",
        #     password="your_authorization_code",
        #     display_name="快速测试",
        #     run_time=30.0
        # )

        print("✅ 快速启动示例完成（未实际运行）")

    except Exception as e:
        print(f"❌ 快速启动示例失败: {e}")


def example_task_creation():
    """任务创建示例"""
    print("\n📧 任务创建示例")
    print("=" * 50)

    try:
        # 1. 创建邮件消息
        email_message = EmailMessage(
            subject="测试邮件",
            recipients=["recipient@example.com"],
            content="这是一封测试邮件",
            content_type="plain"
            # 移除 priority 参数
        )

        # 2. 创建邮件任务
        task = EmailTask(
            task_id=str(uuid.uuid4()),
            message=email_message
            # 移除 priority 参数
        )

        print("✅ 邮件任务创建成功")
        print(f"任务ID: {task.task_id}")
        print(f"邮件主题: {task.message.subject}")
        print(f"收件人: {task.message.recipients}")
        # 移除优先级的显示

        # 3. 演示任务状态管理
        print("\n📊 任务状态管理演示:")

        # 开始处理
        task.start_processing("test_consumer")
        print(f"任务状态: {task.status.value}")
        print(f"处理时间: {task.get_processing_time()}")

        # 模拟成功
        task.mark_success()
        print(f"任务状态: {task.status.value}")
        print(f"总处理时间: {task.get_processing_time()}")

    except Exception as e:
        print(f"❌ 任务创建示例失败: {e}")


def example_config_profiles():
    """配置方案示例"""
    print("\n⚙️ 配置方案示例")
    print("=" * 50)

    try:
        # 1. QQ邮箱配置
        qq_config = ConfigProfiles.qq_email_consumer(
            consumer_id="qq_001",
            username="qq@example.com",
            password="qq_password"
        )
        print("📧 QQ邮箱配置:")
        print(f"  SMTP: {qq_config.email_sender.smtp.host}:{qq_config.email_sender.smtp.port}")
        print(f"  发送间隔: {qq_config.send_interval}秒")

        # 2. Gmail配置
        gmail_config = ConfigProfiles.gmail_consumer(
            consumer_id="gmail_001",
            username="gmail@example.com",
            password="gmail_password"
        )
        print("\n📧 Gmail配置:")
        print(f"  SMTP: {gmail_config.email_sender.smtp.host}:{gmail_config.email_sender.smtp.port}")
        print(f"  发送间隔: {gmail_config.send_interval}秒")

        # 3. 保守配置
        conservative_config = ConfigProfiles.conservative_consumer(
            consumer_id="conservative_001",
            username="conservative@example.com",
            password="conservative_password"
        )
        print("\n📧 保守配置:")
        print(f"  SMTP: {conservative_config.email_sender.smtp.host}:{conservative_config.email_sender.smtp.port}")
        print(f"  发送间隔: {conservative_config.send_interval}秒")

        print("\n✅ 所有配置方案加载成功")

    except Exception as e:
        print(f"❌ 配置方案示例失败: {e}")


def main():
    """主函数"""
    print("🎯 新邮件模块使用示例")
    print("=" * 60)

    try:
        # 运行模拟生产者-消费者示例
        example_producer_consumer()

        # 运行其他示例（可选）
        # example_basic_usage()
        # example_custom_config()
        # example_quick_start()
        # example_task_creation()
        # example_config_profiles()

        print("\n" + "=" * 60)
        print("✅ 所有示例运行完成！")
        print("\n💡 使用建议:")
        print("   1. 根据实际需求选择合适的配置方案")
        print("   2. 注意邮箱授权码的安全性")
        print("   3. 合理设置发送频率避免被限制")
        print("   4. 监控系统状态确保稳定运行")

    except Exception as e:
        print(f"\n❌ 示例运行过程中出现错误: {e}")
        print("请检查模块导入和配置是否正确")


if __name__ == "__main__":
    from loguru import logger

    logger.add("email_system.log", rotation="10 MB", retention="1 day")
    main()
    # print(EmailMessage("测试",['1410959463@qq.com'],"内容"))
