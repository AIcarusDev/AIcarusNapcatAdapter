# aicarus_napcat_adapter/src/utils.py
# Adapter 项目专属的工具函数，现在是名副其实的“神之手”工具箱！
import asyncio
import base64
import json
import os
import ssl
import tempfile
import uuid
from typing import TYPE_CHECKING, Any
from urllib.parse import quote, urlsplit, urlunsplit

import aiohttp
import numpy as np
from aicarus_protocols import ConversationType, Seg
from moviepy.video.io.ImageSequenceClip import ImageSequenceClip
from PIL import Image, ImageSequence

from .logger import logger
from .media_cache_manager import media_cache_manager
from .message_queue import get_napcat_api_response

if TYPE_CHECKING:
    from .aic_com_layer import core_connection_client


# 基于Gemini官方文档的最佳实践
MAX_RESOLUTION = (512, 512)
TARGET_FPS = 5  # 降至5 FPS，略高于Gemini的默认值，捕捉动态同时保持高效率
MAX_DURATION_SECONDS = 10  # 暂时限制最大时长以控制Token，直到找到解决方案


# --- Napcat API 调用辅助函数 (保持不变) ---
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


# --- [NEW] 全能图片处理器 ---
async def process_image_url_to_aicarus_seg(image_url: str, file_id: str | None = None) -> Seg:
    """下载并处理一个图片URL，集成哈希计算、本地缓存和按需发送逻辑.

    - 为原始文件内容计算SHA256哈希。
    - 使用 media_cache_manager 将文件存入本地缓存，并记录哈希。
    - 检查哈希是否已发送过，决定是否在Seg中包含Base64数据。
    - 如果是动图，转换为MP4。
    - 返回一个包含所有必要信息的 image, video, 或 image_failed Seg。
    """
    logger.info(f"启动全能图片处理任务, URL: {image_url}")
    temp_image_path: str | None = None
    temp_mp4_path: str | None = None
    original_image_bytes: bytes | None = None

    ssl_context = ssl.create_default_context()
    ssl_context.set_ciphers("DEFAULT@SECLEVEL=1")

    try:
        # 1. URL 规范化
        try:
            parts = urlsplit(image_url)
            path = quote(parts.path)
            query = quote(parts.query, safe='=&')
            safe_url = urlunsplit((parts.scheme, parts.netloc, path, query, parts.fragment))
            if safe_url != image_url:
                logger.debug(f"URL 已规范化: {image_url} -> {safe_url}")
        except Exception as e:
            logger.error(f"URL 解析失败: {image_url}, 错误: {e}")
            return Seg(
                type="image_failed",
                data={
                    "reason": "Invalid URL",
                    "details": f"URL parsing error: {e}",
                    "url": image_url,
                },
            )

        # 2. 下载原始文件
        headers = {
            "User-Agent": "QQ/9.7.13.29121",
            "Referer": "https://c.tenpay.com/",
            "Accept": "*/*",
        }
        async with aiohttp.ClientSession(
            connector=aiohttp.TCPConnector(ssl=ssl_context)
        ) as session:
            (
                temp_image_path,
                original_image_bytes,
                status_code,
                error_reason,
            ) = await _download_file_to_temp(safe_url, session, headers)
            if not temp_image_path or not original_image_bytes:
                logger.error(
                    f"图片下载失败. URL: {image_url}, "
                    f"Status: {status_code}, Reason: {error_reason}"
                )
                return Seg(
                    type="image_failed",
                    data={
                        "reason": f"HTTP Error {status_code}" if status_code else "Download Error",
                        "details": str(error_reason)[:200],  # 限制长度
                        "url": image_url,
                    },
                )

        # 3. 保存到媒体缓存并获取哈希
        content_type = await get_content_type_from_url(safe_url) or "application/octet-stream"
        content_hash, _ = await media_cache_manager.save_media(original_image_bytes, content_type)
        logger.info(f"媒体文件已保存至本地缓存，哈希: {content_hash[:10]}...")

        # 4. 决定是否需要发送Base64
        should_send_base64 = not core_connection_client.is_hash_sent(content_hash)
        if should_send_base64:
            logger.info(f"哈希 {content_hash[:10]}... 是首次发送给Core，将包含Base64数据。")
        else:
            logger.info(f"哈希 {content_hash[:10]}... 已发送过，本次只发送哈希。")

        # 5. 在线程中进行耗时的图像处理
        def process_image_sync() -> Seg:
            nonlocal temp_mp4_path
            try:
                with Image.open(temp_image_path) as im:
                    is_animated = (
                        getattr(im, "is_animated", False)
                        or getattr(im, "n_frames", 1) > 1
                    )
                    if is_animated:
                        logger.info("Pillow识别为动图，开始转换为MP4...")
                        resized_frames = [
                            np.array(
                                frame
                                .convert("RGBA")
                                .resize(MAX_RESOLUTION, Image.Resampling.LANCZOS)
                                .convert("RGB")
                            )
                            for frame in ImageSequence.Iterator(im)
                        ]

                        if not resized_frames:
                            raise ValueError("未能从动图文件中提取出任何帧。")

                        duration_ms = im.info.get("duration", 100)  # 默认10fps
                        source_fps = (
                            1000.0 / duration_ms
                            if isinstance(duration_ms, int) and duration_ms > 0
                            else TARGET_FPS
                        )
                        final_fps = min(source_fps, TARGET_FPS)
                        total_duration = min(
                            len(resized_frames) / final_fps, MAX_DURATION_SECONDS
                        )
                        clip = ImageSequenceClip(resized_frames, fps=final_fps).set_duration(
                            total_duration
                        )
                        with tempfile.NamedTemporaryFile(delete=False, suffix=".mp4") as temp_f:
                            temp_mp4_path = temp_f.name

                        # 优化write_videofile参数
                        clip.write_videofile(
                            temp_mp4_path,
                            codec="libx264",
                            audio=False,
                            bitrate="500k",  # 较低的比特率以减小文件大小
                            logger=None,
                            preset="medium",  # 平衡的预设，提供良好的压缩率
                            threads=2,
                            ffmpeg_params=[
                                "-pix_fmt",
                                "yuv420p",  # 保证最佳兼容性
                            ],
                        )
                        clip.close()

                        with open(temp_mp4_path, "rb") as mp4_file:
                            mp4_bytes = mp4_file.read()

                        logger.success(f"动图成功转换为MP4, 大小: {len(mp4_bytes)/1024:.2f} KB")

                        seg_data = {
                            "summary": "animated_sticker",
                            "mime_type": "video/mp4",
                            "file_id": file_id,
                            "hash": content_hash,
                            "url": image_url,
                        }
                        if should_send_base64:
                            seg_data["base64"] = base64.b64encode(mp4_bytes).decode("utf-8")

                        return Seg(type="video", data=seg_data)
                    else:
                        # 静态图处理逻辑
                        logger.info("Pillow识别为静态图。")
                        seg_data = {
                            "summary": "image",
                            "mime_type": content_type,
                            "url": image_url,
                            "file_id": file_id,
                            "hash": content_hash,
                        }
                        if should_send_base64:
                            seg_data["base64"] = base64.b64encode(
                                original_image_bytes
                            ).decode("utf-8")

                        return Seg(type="image", data=seg_data)
            except Exception as e_proc:
                logger.error(f"处理图片文件时发生内部错误: {e_proc}", exc_info=True)
                return Seg(
                    type="image_failed",
                    data={
                        "reason": "Processing Error",
                        "details": str(e_proc),
                        "url": image_url
                    }
                )

        # 6. 执行同步处理并获取最终的Seg
        final_seg = await asyncio.to_thread(process_image_sync)

        # 7. 如果成功发送了Base64，则标记
        if should_send_base64 and final_seg.type != "image_failed":
            core_connection_client.mark_hash_as_sent(content_hash)

        return final_seg

    except Exception as e:
        logger.error(f"全能图片处理任务发生严重错误: {e}", exc_info=True)
        return Seg(
            type="image_failed",
            data={
                "reason": "Unhandled Exception",
                "details": str(e),
                "url": image_url
            }
        )
    finally:
        # 8. 清理临时文件
        if temp_image_path and os.path.exists(temp_image_path):
            os.remove(temp_image_path)
        if temp_mp4_path and os.path.exists(temp_mp4_path):
            os.remove(temp_mp4_path)


# --- 其他函数 ---

async def _download_file_to_temp(
    url: str, session: aiohttp.ClientSession, headers: dict | None = None
) -> tuple[str | None, bytes | None, int | None, str | None]:
    """下载文件到临时目录并返回路径、原始字节、HTTP状态码和错误原因."""
    try:
        async with session.get(url, headers=headers) as response:
            if response.status == 200:
                content_bytes = await response.read()
                with tempfile.NamedTemporaryFile(delete=False, suffix=".tmp") as temp_file:
                    temp_file.write(content_bytes)
                    return (
                        temp_file.name,
                        content_bytes,
                        response.status,
                        None,
                    )
            else:
                error_text = await response.text()
                logger.error(
                    f"下载文件失败: {url}, HTTP状态码: {response.status}, "
                    f"服务器响应: {error_text[:200]}"
                )
                return None, None, response.status, error_text
    except aiohttp.ClientError as e:
        status = getattr(e, 'status', None)
        logger.error(
            f"下载文件时发生网络客户端错误: {url}, Status: {status}, Message: {e}",
            exc_info=True,
        )
        return None, None, status, str(e)
    except Exception as e:
        logger.error(f"下载文件时发生未知异常: {url}, 错误: {e}", exc_info=True)
        return None, None, None, str(e)


async def get_content_type_from_url(url: str, timeout: int = 5) -> str | None:
    """通过发送 HEAD 请求，高效地获取 URL 对应资源的 Content-Type."""
    if not url:
        return None

    ssl_context = ssl.create_default_context()
    ssl_context.set_ciphers("DEFAULT@SECLEVEL=1")

    try:
        async with (
            aiohttp.ClientSession(connector=aiohttp.TCPConnector(ssl=ssl_context)) as session,
            session.head(url, timeout=timeout, allow_redirects=True) as response,
        ):
            # 使用 HEAD 请求，只获取响应头，不下载文件体，非常高效
            if response.status == 200:
                content_type = response.headers.get("Content-Type")
                if content_type:
                    # 清理掉可能存在的 charset 等附加信息
                    return content_type.split(";")[0].strip()
                else:
                    logger.warning(f"获取 Content-Type 失败 (HTTP {response.status}): {url}")
                    return None
    except TimeoutError:
        logger.warning(f"获取 Content-Type 超时: {url}")
        return None
    except Exception as e:
        logger.error(f"获取 Content-Type 时发生未知错误 (URL: {url}): {e}")
        return None


# --- 所有 napcat_ 开头的 API 函数保持原样 ---
# ... (此处省略所有 napcat_... 函数, 请保留你文件中的这些函数)
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


async def napcat_delete_friend(server_connection: Any, **kwargs: Any) -> dict[str, Any] | None:
    """删除好友的统一入口."""
    params = {
        "user_id": int(kwargs["user_id"]),
    }
    return await _call_napcat_api(server_connection, "delete_friend", params)


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
    return await _call_napcat_api(server_connection, "_send_group_notice", params)


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

async def napcat_get_bot_profile_for_core(
    server_connection: Any, **kwargs: Any
) -> dict[str, Any] | None:
    """一个强大的复合函数，获取机器人自身、好友列表和群聊列表，并组合成Core需要的完整档案。."""
    logger.info("安检流程: 正在执行 get_bot_profile_for_core...")

    # 使用 asyncio.gather 并发执行所有需要的 API 调用
    profile_task = _call_napcat_api(server_connection, "get_login_info", {})
    friends_task = _call_napcat_api(server_connection, "get_friend_list", {})
    groups_task = _call_napcat_api(server_connection, "get_group_list", {})

    results = await asyncio.gather(profile_task, friends_task, groups_task)

    profile_data, friends_list, groups_list = results

    if not profile_data:
        logger.error("安检失败: 未能获取到基本的机器人信息 (get_login_info)。")
        return None

    # 将群聊列表转换为 Napcat API 返回的那种以 group_id 为键的字典格式
    groups_dict = {
        str(group.get("group_id", "")): group for group in groups_list
    } if groups_list else {}

    # 组装成 Core 需要的最终格式
    full_profile = {
        "user_id": profile_data.get("user_id"),
        "user_nickname": profile_data.get("nickname"),
        "friends": friends_list if friends_list else [],
        "groups": groups_dict
    }

    logger.info(
        "安检流程: 成功组装了 "
        f"{len(full_profile['friends'])} 位好友和 "
        f"{len(full_profile['groups'])} 个群聊的完整档案。"
    )
    return full_profile
