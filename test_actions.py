# test_actions.py (最终修正版 v2 - 彻底分离输入与事件循环)
import asyncio
import json
import threading
import time
import uuid

import websockets

# --- 配置 ---
CORE_HOST = "127.0.0.1"
CORE_PORT = 8077

# --- 全局状态 ---
CONNECTED_ADAPTER: websockets.WebSocketServerProtocol | None = None
SHUTDOWN_EVENT = threading.Event()  # 使用线程安全的 Event
PLATFORM_ID = ""


def build_action_event(action_alias: str, params: dict) -> dict:
    """构建一个标准的 AIcarus 动作事件."""
    global PLATFORM_ID
    return {
        "event_id": f"test_action_{uuid.uuid4()}",
        "event_type": f"action.{PLATFORM_ID}.{action_alias}",
        "time": int(time.time() * 1000),
        "bot_id": "test_bot",
        "user_info": None,
        "conversation_info": None,
        "content": [{"type": "action_params", "data": params}],
        "raw_data": "{}",
    }

async def handle_adapter_connection(
        websocket: websockets.WebSocketServerProtocol, path: str
    ) -> None:
    """处理来自 Adapter 的连接."""
    global CONNECTED_ADAPTER
    if CONNECTED_ADAPTER is not None and CONNECTED_ADAPTER.open:
        print("[模拟Core] 警告: 已有一个 Adapter 连接，新的连接将被拒绝。")
        await websocket.close(1013, "Service temporarily unavailable")
        return

    CONNECTED_ADAPTER = websocket
    print(f"\n[模拟Core] ✅ Adapter 已连接: {websocket.remote_address}")

    try:
        async for message in websocket:
            try:
                event_data = json.loads(message)
                event_type = event_data.get("event_type", "unknown")
                print(f"[模拟Core] <- 收到事件: {event_type}")
            except json.JSONDecodeError:
                print(f"[模拟Core] <- 收到无法解析的原始消息: {message[:100]}...")
    except websockets.exceptions.ConnectionClosed:
        print(f"\n[模拟Core] ❌ Adapter 连接已断开: {websocket.remote_address}")
    finally:
        CONNECTED_ADAPTER = None
        if not SHUTDOWN_EVENT.is_set():
            print("[模拟Core] Adapter 连接意外断开，测试脚本将退出。")
            SHUTDOWN_EVENT.set()

def user_input_loop(loop: asyncio.AbstractEventLoop) -> None:
    """在一个独立的线程中处理用户输入，避免阻塞事件循环."""
    global PLATFORM_ID

    # 等待 Adapter 连接成功
    while CONNECTED_ADAPTER is None and not SHUTDOWN_EVENT.is_set():
        threading.Event().wait(0.5)

    if SHUTDOWN_EVENT.is_set():
        return

    # 动态获取 platform_id
    try:
        import tomlkit
        with open("config.toml", encoding="utf-8") as f:
            config_data = tomlkit.load(f)
            PLATFORM_ID = config_data["core_connection"]["platform_id"]
            print(f"[模拟Core] 已从 config.toml 自动读取 platform_id: {PLATFORM_ID}")
    except Exception:
        print("[模拟Core] 警告: 无法自动读取 config.toml, 将使用默认值 'qq'。")
        PLATFORM_ID = "qq"

    while not SHUTDOWN_EVENT.is_set():
        print("\n=====================================")
        print("  AIcarus Napcat Adapter 动作测试 (Core 模拟器)")
        print("=====================================")
        print("  1. 主动添加好友 (add_friend)")
        print("  2. 主动申请加群 (join_group)")
        print("  q. 退出")

        try:
            choice = input("请选择要测试的功能: ").strip()
        except (EOFError, KeyboardInterrupt):
            print("\n[模拟Core] 检测到输入中断，正在退出...")
            SHUTDOWN_EVENT.set()
            break

        if CONNECTED_ADAPTER is None or not CONNECTED_ADAPTER.open:
            print("[模拟Core] 错误: 与 Adapter 的连接已断开。")
            SHUTDOWN_EVENT.set()
            break

        action_event = None
        if choice == '1':
            user_id = input("  请输入要添加的好友QQ号: ").strip()
            comment = input("  请输入验证信息 (可留空): ").strip()
            if not user_id.isdigit():
                print("  错误: QQ号必须是数字。")
                continue
            action_event = build_action_event(
                "add_friend", {"user_id": user_id, "comment": comment}
            )

        elif choice == '2':
            group_id = input("  请输入要加入的群号: ").strip()
            comment = input("  请输入验证信息 (可留空): ").strip()
            if not group_id.isdigit():
                print("  错误: 群号必须是数字。")
                continue
            action_event = build_action_event(
                "join_group", {"group_id": group_id, "comment": comment}
            )

        elif choice.lower() == 'q':
            print("[模拟Core] 用户请求退出...")
            SHUTDOWN_EVENT.set()
            break
        else:
            print("无效的输入，请重新选择。")
            continue

        if action_event:
            future = asyncio.run_coroutine_threadsafe(
                CONNECTED_ADAPTER.send(json.dumps(action_event)),
                loop
            )
            try:
                future.result(timeout=5)  # 等待发送完成，设置超时
                print(f"[模拟Core] -> 已发送动作: {action_event['event_type']}")
            except Exception as e:
                print(f"[模拟Core] 错误: 发送动作到事件循环时失败: {e}")
                SHUTDOWN_EVENT.set()
                break

async def main() -> None:
    """主异步函数."""
    loop = asyncio.get_running_loop()

    # 启动用户输入线程
    input_thread = threading.Thread(target=user_input_loop, args=(loop,), daemon=True)
    input_thread.start()

    # 启动 WebSocket 服务器
    async with websockets.serve(handle_adapter_connection, CORE_HOST, CORE_PORT):
        print(f"[模拟Core] 服务器已启动，正在 http://{CORE_HOST}:{CORE_PORT} 等待 Adapter 连接...")

        # 等待关闭信号
        await loop.run_in_executor(None, SHUTDOWN_EVENT.wait)

    print("[模拟Core] 服务器已关闭。")

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n[模拟Core] 脚本被强制中断。")
    finally:
        print("[模拟Core] 测试脚本执行完毕。")
