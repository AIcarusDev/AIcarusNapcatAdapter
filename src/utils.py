# aicarus_napcat_adapter/src/utils.py
# Adapter 项目专属的工具函数，现在是名副其实的“神之手”工具箱！
import base64
import json
import ssl
import uuid
from typing import Any

import aiohttp
from aicarus_protocols import ConversationType  # 导入会话类型常量

from .logger import logger
from .message_queue import get_napcat_api_response


# --- Napcat API 调用辅助函数 ---
async def _call_napcat_api(
    server_connection: Any,
    action: str,
    params: dict[str, Any],
    timeout_seconds: float = 15.0,
) -> dict[str, Any] | None:
    """通用的 Napcat API 调用函数。现在它是所有神之手的力量源泉!"""
    if not server_connection or server_connection.closed:
        logger.error(f"无法调用 Napcat API '{action}': WebSocket 连接不可用或已关闭。")
        return None

    request_echo_id = str(uuid.uuid4())
    payload = {"action": action, "params": params, "echo": request_echo_id}

    try:
        logger.debug(
            f"向 Napcat 发送 API 请求: action='{action}', params={params}, echo='{request_echo_id}'"
        )
        await server_connection.send(json.dumps(payload))
        response_data = await get_napcat_api_response(
            request_echo_id, timeout_seconds=timeout_seconds
        )

        if response_data and response_data.get("status") == "ok":
            logger.debug(f"Napcat API '{action}' 调用成功。响应: {response_data.get('data')}")
            return response_data.get("data") if response_data.get("data") is not None else {}
        else:
            error_msg = (
                response_data.get("message", "未知错误")
                if response_data
                else "无响应或响应格式错误"
            )
            retcode = response_data.get("retcode", "N/A") if response_data else "N/A"
            logger.warning(
                f"Napcat API '{action}' 调用失败。Status: {response_data.get('status')}, "
                f"Retcode: {retcode}, Message: {error_msg}"
            )
            return None
    except TimeoutError:
        logger.error(f"调用 Napcat API '{action}' 超时 ({timeout_seconds}s)。")
        return None
    except Exception as e:
        logger.error(f"调用 Napcat API '{action}' 时发生异常: {e}", exc_info=True)
        return None


# --- 信息获取类 ---
async def napcat_get_self_info(server_connection: Any, **kwargs: Any) -> dict[str, Any] | None:
    """获取当前登录用户的信息."""
    return await _call_napcat_api(server_connection, "get_login_info", {})


async def napcat_get_group_info(server_connection: Any, **kwargs: Any) -> dict[str, Any] | None:
    """获取群组信息."""
    return await _call_napcat_api(
        server_connection, "get_group_info", {"group_id": int(kwargs["group_id"])}
    )


async def napcat_get_member_info(server_connection: Any, **kwargs: Any) -> dict[str, Any] | None:
    """获取群成员信息的统一入口."""
    params = {
        "group_id": int(kwargs["group_id"]),
        "user_id": int(kwargs["user_id"]),
        "no_cache": kwargs.get("no_cache", False),
    }
    return await _call_napcat_api(server_connection, "get_group_member_info", params)


async def napcat_get_stranger_info(server_connection: Any, **kwargs: Any) -> dict[str, Any] | None:
    """获取陌生人信息的统一入口."""
    params = {
        "user_id": int(kwargs["user_id"]),
        "no_cache": kwargs.get("no_cache", False),
    }
    return await _call_napcat_api(server_connection, "get_stranger_info", params)


async def napcat_get_list(server_connection: Any, **kwargs: Any) -> dict[str, Any] | None:
    """获取好友或群列表的统一入口."""
    list_type = kwargs.get("list_type")
    if list_type == "friend":
        return await _call_napcat_api(server_connection, "get_friend_list", {})
    elif list_type == "group":
        return await _call_napcat_api(server_connection, "get_group_list", {})
    logger.warning(f"未知的 get_list 类型: {list_type}")
    return None


async def napcat_get_history(server_connection: Any, **kwargs: Any) -> dict[str, Any] | None:
    """获取历史消息的统一入口."""
    conv_id = kwargs["conversation_id"]
    conv_type = kwargs["conversation_type"]
    params: dict[str, Any] = {"count": kwargs.get("count", 20)}
    if "message_seq" in kwargs:
        params["message_seq"] = kwargs["message_seq"]

    if conv_type == ConversationType.GROUP:
        params["group_id"] = int(conv_id)
        return await _call_napcat_api(
            server_connection, "get_group_msg_history", params, timeout_seconds=30
        )
    elif conv_type == ConversationType.PRIVATE:
        params["user_id"] = int(conv_id)
        return await _call_napcat_api(
            server_connection, "get_friend_msg_history", params, timeout_seconds=30
        )
    return None


# --- 好友操作类 ---
async def napcat_delete_friend(server_connection: Any, **kwargs: Any) -> dict[str, Any] | None:
    """删除好友的统一入口."""
    params = {
        "user_id": int(kwargs["user_id"]),
    }
    # 根据 gocq_api.md，这个接口叫 delete_friend，不是 set_delete_friend
    return await _call_napcat_api(server_connection, "delete_friend", params)


# --- 群组管理类 ---
async def napcat_set_group_kick(server_connection: Any, **kwargs: Any) -> dict[str, Any] | None:
    """踢出群成员的统一入口."""
    params = {
        "group_id": int(kwargs["group_id"]),
        "user_id": int(kwargs["user_id"]),
        "reject_add_request": kwargs.get("reject_add_request", False),
    }
    return await _call_napcat_api(server_connection, "set_group_kick", params)


async def napcat_set_group_ban(server_connection: Any, **kwargs: Any) -> dict[str, Any] | None:
    """禁言群成员的统一入口."""
    params = {
        "group_id": int(kwargs["group_id"]),
        "user_id": int(kwargs["user_id"]),
        "duration": kwargs.get("duration", 1800),
    }
    return await _call_napcat_api(server_connection, "set_group_ban", params)


async def napcat_set_group_whole_ban(
    server_connection: Any, **kwargs: Any
) -> dict[str, Any] | None:
    """设置群全员禁言的统一入口."""
    params = {"group_id": int(kwargs["group_id"]), "enable": kwargs["enable"]}
    return await _call_napcat_api(server_connection, "set_group_whole_ban", params)


async def napcat_set_group_card(server_connection: Any, **kwargs: Any) -> dict[str, Any] | None:
    """设置群成员的名片（备注）的统一入口."""
    params = {
        "group_id": int(kwargs["group_id"]),
        "user_id": int(kwargs["user_id"]),
        "card": kwargs.get("card", ""),
    }
    return await _call_napcat_api(server_connection, "set_group_card", params)


async def napcat_set_group_special_title(
    server_connection: Any, **kwargs: Any
) -> dict[str, Any] | None:
    """设置群成员的特殊头衔的统一入口."""
    params = {
        "group_id": int(kwargs["group_id"]),
        "user_id": int(kwargs["user_id"]),
        "special_title": kwargs.get("special_title", ""),
        "duration": kwargs.get("duration", -1),
    }
    return await _call_napcat_api(server_connection, "set_group_special_title", params)


async def napcat_set_group_leave(server_connection: Any, **kwargs: Any) -> dict[str, Any] | None:
    """让机器人或用户离开群组的统一入口."""
    params = {
        "group_id": int(kwargs["group_id"]),
        "is_dismiss": kwargs.get("is_dismiss", False),
    }
    return await _call_napcat_api(server_connection, "set_group_leave", params)


async def napcat_set_group_admin(server_connection: Any, **kwargs: Any) -> dict[str, Any] | None:
    """设置群管理员的统一入口."""
    params = {
        "group_id": int(kwargs["group_id"]),
        "user_id": int(kwargs["user_id"]),
        "enable": kwargs.get("enable", True),
    }
    return await _call_napcat_api(server_connection, "set_group_admin", params)


async def napcat_set_group_name(server_connection: Any, **kwargs: Any) -> dict[str, Any] | None:
    """设置群组名称的统一入口."""
    params = {"group_id": int(kwargs["group_id"]), "group_name": kwargs["group_name"]}
    return await _call_napcat_api(server_connection, "set_group_name", params)


# --- 消息操作类 ---
async def napcat_delete_msg(server_connection: Any, **kwargs: Any) -> dict[str, Any] | None:
    """删除消息的统一入口."""
    return await _call_napcat_api(
        server_connection, "delete_msg", {"message_id": int(kwargs["message_id"])}
    )


async def napcat_send_poke(server_connection: Any, **kwargs: Any) -> dict[str, Any] | None:
    """统一的戳一戳入口."""
    params: dict[str, Any] = {"user_id": int(kwargs["target_user_id"])}
    action = "friend_poke"
    if kwargs.get("group_id"):
        params["group_id"] = int(kwargs["group_id"])
        action = "group_poke"
    return await _call_napcat_api(server_connection, action, params)


async def napcat_set_msg_emoji_like(server_connection: Any, **kwargs: Any) -> dict[str, Any] | None:
    """设置消息表情点赞的统一入口."""
    params = {"message_id": int(kwargs["message_id"]), "emoji_id": kwargs["emoji_id"]}
    return await _call_napcat_api(server_connection, "set_msg_emoji_like", params)


async def napcat_forward_single_msg(server_connection: Any, **kwargs: Any) -> dict[str, Any] | None:
    """转发单条消息的统一入口."""
    target_id = kwargs["target_id"]
    target_type = kwargs["target_type"]
    message_id = kwargs["message_id"]

    if target_type == ConversationType.GROUP:
        params = {"group_id": int(target_id), "message_id": int(message_id)}
        return await _call_napcat_api(server_connection, "forward_group_single_msg", params)
    elif target_type == ConversationType.PRIVATE:
        params = {"user_id": int(target_id), "message_id": int(message_id)}
        return await _call_napcat_api(server_connection, "forward_friend_single_msg", params)
    return None


# --- 请求处理类 ---
async def napcat_set_friend_add_request(
    server_connection: Any, **kwargs: Any
) -> dict[str, Any] | None:
    """处理好友添加请求的统一入口."""
    params = {
        "flag": kwargs["flag"],
        "approve": kwargs["approve"],
        "remark": kwargs.get("remark", ""),
    }
    return await _call_napcat_api(server_connection, "set_friend_add_request", params)


async def napcat_set_group_add_request(
    server_connection: Any, **kwargs: Any
) -> dict[str, Any] | None:
    """处理群添加请求的统一入口."""
    params = {
        "flag": kwargs["flag"],
        "sub_type": kwargs["sub_type"],
        "approve": kwargs["approve"],
        "reason": kwargs.get("reason", ""),
    }
    return await _call_napcat_api(server_connection, "set_group_add_request", params)


# --- 文件操作类 ---
async def napcat_upload_group_file(server_connection: Any, **kwargs: Any) -> dict[str, Any] | None:
    """上传群文件的统一入口."""
    params: dict[str, Any] = {
        "group_id": int(kwargs["group_id"]),
        "file": kwargs["file"],
        "name": kwargs["name"],
    }
    if kwargs.get("folder"):
        params["folder"] = kwargs["folder"]
    return await _call_napcat_api(
        server_connection, "upload_group_file", params, timeout_seconds=300
    )


async def napcat_delete_group_file(server_connection: Any, **kwargs: Any) -> dict[str, Any] | None:
    """删除群文件的统一入口."""
    params = {
        "group_id": int(kwargs["group_id"]),
        "file_id": kwargs["file_id"],
        "busid": int(kwargs["busid"]),
    }
    return await _call_napcat_api(server_connection, "delete_group_file", params)


async def napcat_create_group_file_folder(
    server_connection: Any, **kwargs: Any
) -> dict[str, Any] | None:
    """创建群文件夹的统一入口."""
    params = {
        "group_id": int(kwargs["group_id"]),
        "name": kwargs["name"],
        "parent_id": "/",
    }
    return await _call_napcat_api(server_connection, "create_group_file_folder", params)


async def napcat_delete_group_folder(
    server_connection: Any, **kwargs: Any
) -> dict[str, Any] | None:
    """删除群文件夹的统一入口."""
    params = {"group_id": int(kwargs["group_id"]), "folder_id": kwargs["folder_id"]}
    return await _call_napcat_api(server_connection, "delete_group_folder", params)


async def napcat_get_group_files_by_folder(
    server_connection: Any, **kwargs: Any
) -> dict[str, Any] | None:
    """获取群文件列表的统一入口，支持按文件夹获取."""
    params = {"group_id": int(kwargs["group_id"])}
    if kwargs.get("folder_id"):
        params["folder_id"] = kwargs["folder_id"]
        return await _call_napcat_api(server_connection, "get_group_files_by_folder", params)
    return await _call_napcat_api(server_connection, "get_group_root_files", params)


async def napcat_get_group_file_url(server_connection: Any, **kwargs: Any) -> dict[str, Any] | None:
    """获取群文件的下载链接的统一入口."""
    params = {
        "group_id": int(kwargs["group_id"]),
        "file_id": kwargs["file_id"],
        "busid": int(kwargs["busid"]),
    }
    return await _call_napcat_api(server_connection, "get_group_file_url", params)


# --- 其他功能类 ---
async def napcat_set_group_sign(server_connection: Any, **kwargs: Any) -> dict[str, Any] | None:
    """设置群组签名的统一入口."""
    return await _call_napcat_api(
        server_connection, "set_group_sign", {"group_id": int(kwargs["group_id"])}
    )


async def napcat_set_online_status(server_connection: Any, **kwargs: Any) -> dict[str, Any] | None:
    """设置在线状态的统一入口."""
    params = {
        "status": int(kwargs["status"]),
        "ext_status": int(kwargs.get("ext_status", 0)),
        "battery_status": int(kwargs.get("battery_status", 100)),
    }
    return await _call_napcat_api(server_connection, "set_online_status", params)


async def napcat_set_qq_avatar(server_connection: Any, **kwargs: Any) -> dict[str, Any] | None:
    """设置 QQ 头像的统一入口."""
    return await _call_napcat_api(server_connection, "set_qq_avatar", {"file": kwargs["file"]})


async def napcat_get_group_honor_info(
    server_connection: Any, **kwargs: Any
) -> dict[str, Any] | None:
    """获取群组荣誉信息的统一入口."""
    params = {"group_id": int(kwargs["group_id"]), "type": kwargs.get("type", "all")}
    return await _call_napcat_api(server_connection, "get_group_honor_info", params)


async def napcat_send_group_notice(server_connection: Any, **kwargs: Any) -> dict[str, Any] | None:
    """发送群组公告的统一入口."""
    params: dict[str, Any] = {
        "group_id": int(kwargs["group_id"]),
        "content": kwargs["content"],
    }
    if kwargs.get("image"):
        params["image"] = kwargs["image"]
    return await _call_napcat_api(
        server_connection, "_send_group_notice", params
    )  # 注意gocq的下划线


async def napcat_get_group_notice(server_connection: Any, **kwargs: Any) -> dict[str, Any] | None:
    """获取群组公告的统一入口."""
    return await _call_napcat_api(
        server_connection, "_get_group_notice", {"group_id": int(kwargs["group_id"])}
    )


async def napcat_get_recent_contact(server_connection: Any, **kwargs: Any) -> dict[str, Any] | None:
    """获取最近联系人的统一入口."""
    params = {"count": kwargs.get("count", 20)}
    return await _call_napcat_api(server_connection, "get_recent_contact", params)


async def napcat_get_ai_characters(server_connection: Any, **kwargs: Any) -> dict[str, Any] | None:
    """获取群组 AI 角色的统一入口."""
    params = {"group_id": int(kwargs["group_id"])}
    return await _call_napcat_api(server_connection, "get_ai_characters", params)


async def napcat_send_group_ai_record(
    server_connection: Any, **kwargs: Any
) -> dict[str, Any] | None:
    """发送群组 AI 记录的统一入口."""
    params = {
        "group_id": int(kwargs["group_id"]),
        "character": kwargs["character"],
        "text": kwargs["text"],
    }
    return await _call_napcat_api(server_connection, "send_group_ai_record", params)


async def napcat_get_forward_msg_content(
    server_connection: Any, forward_msg_id: str
) -> list[dict[str, Any]] | None:
    """获取合并转发消息的内容."""
    data = await _call_napcat_api(
        server_connection, "get_forward_msg", {"message_id": forward_msg_id}
    )
    if data and isinstance(data.get("messages"), list):
        return data["messages"]
    elif data:
        logger.warning(
            f"获取合并转发消息 (id: {forward_msg_id}) 内容时，返回的 'messages' "
            f"字段格式不正确: {data.get('messages')}"
        )
    return None


# --- 图片处理工具函数 (这部分保持不变) ---
async def get_image_base64_from_url(url: str, timeout: int = 10) -> str | None:
    """从给定的 URL 下载图片并返回其 Base64 编码字符串."""
    ssl_context = ssl.create_default_context()
    ssl_context.set_ciphers("DEFAULT@SECLEVEL=1")
    try:
        async with aiohttp.ClientSession(  # noqa: SIM117
            connector=aiohttp.TCPConnector(ssl=ssl_context)
        ) as session:
            async with session.get(url, timeout=timeout) as response:
                if response.status == 200:
                    image_bytes = await response.read()
                    return base64.b64encode(image_bytes).decode("utf-8")
                else:
                    logger.error(f"下载图片失败 (HTTP {response.status}): {url}")
                    return None
    except Exception as e:
        logger.error(f"下载或处理图片时发生错误 (URL: {url}): {e}", exc_info=True)
        return None
