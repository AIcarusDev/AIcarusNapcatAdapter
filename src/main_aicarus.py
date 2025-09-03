# AIcarus Napcat Adapter - Main Entry Point for Protocol v1.5.1
# aicarus_napcat_adapter/src/main_aicarus.py
import asyncio
import json
import sys

import websockets  # 确保导入

# v1.5.1 协议库
from aicarus_protocols import (
    PROTOCOL_VERSION,
    EventBuilder,
)

from . import aic_com_layer
from .aic_com_layer import (  # 从新的 v1.5.1 通信层导入
    aic_start_com,  # 这个函数现在会启动 core_connection_client.run_forever()
    aic_stop_com,  # 这个函数会调用 core_connection_client.stop_communication()
)
from .aic_com_layer import (
    router_aicarus as core_router,  # router_aicarus 是 core_connection_client 的实例
)
from .config import (
    AdapterConfigData,
    get_config,  # 添加global_config
)

# 项目内部模块
from .logger import logger

# 从新的消息队列模块导入 (如果 napcat_event_processor 仍使用它)
from .message_queue import (
    check_stale_api_responses_periodically,
    internal_event_queue,
    put_napcat_api_response,
)

# 直接导入 recv_handler_aicarus 实例，而不是类
from .recv_handler_aicarus import recv_handler_aicarus
from .send_handler_aicarus import send_handler_aicarus

# recv_handler_aicarus 实例已在其模块中创建并导入，此处无需再创建


async def napcat_message_receiver(
    websocket: websockets.WebSocketServerProtocol, path: str
) -> None:
    """处理来自 Napcat 的连接和消息，并将消息分发给 RecvHandlerAicarus."""
    logger.info(f"Napcat 客户端已连接: {websocket.remote_address}")

    # --- 核心修改 1：将 Napcat 连接注入通信层 ---
    aic_com_layer.core_connection_client.set_napcat_server_connection(websocket)

    recv_handler_aicarus.server_connection = websocket
    send_handler_aicarus.server_connection = websocket


    # ------------------ 1: 接入 Core ------------------
    # 在确认QQ已连接后，我们才开始启动与Core的连接
    logger.info("QQ已就位，正在尝试与 Core 建立联系...")
    core_com_task = asyncio.create_task(aic_start_com())
    # -----------------------------------------------------------

    try:
        async for raw_message_str in websocket:
            logger.debug(f"AIcarus Adapter: Raw from Napcat: {raw_message_str[:120]}...")
            try:
                napcat_event: dict = json.loads(raw_message_str)
            except json.JSONDecodeError:
                logger.error(
                    f"AIcarus Adapter: Failed to decode JSON from Napcat: {raw_message_str}"
                )
                continue

            post_type = napcat_event.get("post_type")

            # 我们只关心这几种类型的事件，直接把它们丢给事件处理器队列
            if post_type in ["meta_event", "message", "notice", "request"]:
                await internal_event_queue.put(napcat_event)
            # 我们也关心 Napcat API 的响应
            elif napcat_event.get("echo"):
                await put_napcat_api_response(napcat_event)
            # 对于其他所有类型的 post_type (包括 message_sent)，我们直接忽略，让它们随风而去~
            else:
                logger.debug(
                    f"AIcarus Adapter: Ignoring Napcat event with post_type '{post_type}'."
                )

    except websockets.exceptions.ConnectionClosedOK:
        logger.info(f"Napcat client {websocket.remote_address} disconnected gracefully.")
    except websockets.exceptions.ConnectionClosedError as e:
        logger.warning(
            f"Napcat client {websocket.remote_address} connection closed with error: {e}"
        )
    except Exception as e:
        logger.error(
            f"处理 Napcat 连接时发生未知错误 ({websocket.remote_address}): {e}",
            exc_info=True,
        )
    finally:
        logger.info(f"Napcat 客户端连接已结束: {websocket.remote_address}")
        if recv_handler_aicarus.server_connection == websocket:
            recv_handler_aicarus.server_connection = None
        if send_handler_aicarus.server_connection == websocket:
            send_handler_aicarus.server_connection = None

        # ------------------ 2: 与Core分离 ------------------
        # 当QQ连接断开，我们也要优雅地从Core中断开
        logger.info("QQ已离开，正在与Core断开连接...")
        await aic_stop_com()
        if core_com_task and not core_com_task.done():
            core_com_task.cancel()  # 确保任务被取消
        logger.info("已完全从Core断开连接。")
        # -----------------------------------------------------------


async def napcat_event_processor() -> None:
    """从内部队列中取出 Napcat 事件并分发给 RecvHandlerAicarus 的统一入口."""
    logger.info("Napcat 事件处理器已启动，等待处理事件... (工厂模式)")
    while True:
        napcat_event = await internal_event_queue.get()
        try:
            # 无需再判断 post_type，因为我们已经在接收器中筛选了
            await recv_handler_aicarus.process_event(napcat_event)
        except Exception as e:
            post_type = napcat_event.get("post_type", "unknown")
            logger.error(
                f"AIcarus Adapter: Error processing Napcat event (post_type: {post_type}): {e}",
                exc_info=True,
            )
        finally:
            internal_event_queue.task_done()


async def start_napcat_websocket_server(config: AdapterConfigData) -> None:
    """启动 WebSocket 服务器，等待 Napcat 连接."""
    logger.info(
        f"启动 AIcarus Napcat Adapter WebSocket 服务器 (Protocol v{PROTOCOL_VERSION}): "
        f"{config.adapter_server_host}:{config.adapter_server_port}"
    )

    try:
        async with websockets.serve(
            napcat_message_receiver,
            config.adapter_server_host,
            config.adapter_server_port,
            ping_interval=20,
            ping_timeout=10,
            max_size=2**20,  # 1MB max message size
        ):
            logger.info(
                f"AIcarus Napcat Adapter WebSocket 服务器已启动，等待 Napcat 连接... "
                f"(Protocol v{PROTOCOL_VERSION})"
            )
            await asyncio.Future()  # 永远运行
    except Exception as e:
        logger.critical(f"启动 Napcat WebSocket 服务器时发生错误: {e}", exc_info=True)
        sys.exit(1)


async def main() -> None:
    """主函数，启动所有组件."""
    config = get_config()
    logger.info(f"AIcarus Napcat Adapter 正在启动... (Protocol v{PROTOCOL_VERSION})")
    logger.info(f"配置: Napcat 连接端口 {config.adapter_server_port}")
    logger.info(f"配置: Core 连接 URL {config.core_connection_url}")

    # 将 recv_handler 的 router 设置为 core_router，使其能够向 Core 发送消息
    recv_handler_aicarus.router = core_router
    logger.info("已将 recv_handler 的 Core 路由器配置完成。")

    # 注册 send_handler 的回调到 Core 通信层，使其能够接收来自 Core 的动作指令
    core_router.register_core_event_handler(send_handler_aicarus.handle_aicarus_action)
    logger.info("已注册 send_handler 为 Core 事件处理回调。")

    # 启动 Napcat 事件处理器（异步任务）
    logger.info("启动 Napcat 事件处理器...")
    napcat_processor_task = asyncio.create_task(napcat_event_processor())

    # 启动过期 API 响应检查器（异步任务）
    logger.info("启动过期 API 响应检查器...")
    stale_check_task = asyncio.create_task(check_stale_api_responses_periodically())

    # 启动 WebSocket 服务器等待 Napcat 连接（这会阻塞）
    try:
        await start_napcat_websocket_server(config)
    except KeyboardInterrupt:
        logger.info("收到中断信号，正在关闭 AIcarus Napcat Adapter...")
    except Exception as e:
        logger.critical(f"Adapter 运行时发生致命错误: {e}", exc_info=True)
    finally:
        # 清理和关闭
        logger.info("正在关闭所有组件...")

        # 取消异步任务
        if napcat_processor_task and not napcat_processor_task.done():
            napcat_processor_task.cancel()
            try:
                await napcat_processor_task
            except asyncio.CancelledError:
                logger.info("Napcat 事件处理器已停止。")

        if stale_check_task and not stale_check_task.done():
            stale_check_task.cancel()
            try:
                await stale_check_task
            except asyncio.CancelledError:
                logger.info("过期响应检查器已停止。")

        # 停止与 Core 的通信
        await aic_stop_com()

        logger.info("AIcarus Napcat Adapter 已完全关闭。")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("程序被用户中断。")
    except Exception as e:
        logger.critical(f"程序启动失败: {e}", exc_info=True)
        sys.exit(1)
