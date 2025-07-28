# aicarus_napcat_adapter/src/send_handler_aicarus.py (v3.0 重构版)
import json
import uuid
from collections.abc import Callable
from typing import Any

import websockets

# AIcarus 协议库
from aicarus_protocols import Event, EventBuilder, Seg, find_seg_by_type

# 哼哼，从我们重构好的新世界里导入！
from .action_definitions import ACTION_MAPPING, COMPLEX_ACTION_HANDLERS
from .action_register import pending_actions

# 内部模块
from .logger import logger
from .message_queue import get_napcat_api_response
from .napcat_definitions import NapcatSegType
from .recv_handler_aicarus import recv_handler_aicarus


class SendHandlerAicarus:
    """这是一个专门为AIcarus设计的发送处理器."""

    def __init__(self) -> None:
        self.server_connection: websockets.WebSocketServerProtocol | None = None
        self.SEGMENT_CONVERTERS: dict[str, Callable[[Seg], dict[str, Any] | None]] = {
            "text": self._convert_text_seg,
            "at": self._convert_at_seg,
            "reply": self._convert_reply_seg,
            "quote": self._convert_reply_seg,
            "image": self._convert_image_seg,
            "face": self._convert_face_seg,
            "record": self._convert_record_seg,
            "video": self._convert_video_seg,
            "file": self._convert_file_seg,
            "contact": self._convert_contact_seg,
            "music": self._convert_music_seg,
        }

    # --- 这是各种工具的具体实现 ---

    def _convert_text_seg(self, seg: Seg) -> dict[str, Any] | None:
        """处理文本消息."""
        return {
            "type": NapcatSegType.text,
            "data": {"text": str(seg.data.get("text", ""))},
        }

    def _convert_at_seg(self, seg: Seg) -> dict[str, Any] | None:
        """处理@消息，必须有 user_id."""
        target_qq = seg.data.get("user_id")
        if not target_qq:
            logger.warning("发送@失败：Seg段中缺少 user_id。")
            return None
        return {
            "type": NapcatSegType.at,
            "data": {"qq": str(target_qq)},
        }

    def _convert_reply_seg(self, seg: Seg) -> dict[str, Any] | None:
        """处理回复消息，必须有 message_id."""
        msg_id = seg.data.get("message_id")
        if not msg_id:
            logger.warning("发送回复失败：Seg段中缺少 message_id。")
            return None
        return {
            "type": NapcatSegType.reply,
            "data": {"id": str(msg_id)},
        }

    def _convert_image_seg(self, seg: Seg) -> dict[str, Any] | None:
        """处理图片消息，支持多种来源."""
        file_source = (
            seg.data.get("file")
            or seg.data.get("file_id")
            or seg.data.get("url")
            or seg.data.get("base64")
        )
        if not file_source:
            logger.warning("发送图片失败：Seg段中缺少 file, file_id, url 或 base64。")
            return None
        return {"type": NapcatSegType.image, "data": {"file": file_source}}

    def _convert_face_seg(self, seg: Seg) -> dict[str, Any] | None:
        """QQ表情消息，必须有 id."""
        face_id = seg.data.get("id")
        if face_id is None:
            logger.warning("发送表情失败：Seg段中缺少 id。")
            return None
        return {"type": NapcatSegType.face, "data": {"id": str(face_id)}}

    def _convert_media_seg(self, seg: Seg, napcat_type: str) -> dict[str, Any] | None:
        """处理媒体消息（语音、视频、文件等）."""
        file_source = seg.data.get("file") or seg.data.get("url") or seg.data.get("path")
        if not file_source:
            logger.warning(f"发送{napcat_type}失败：Seg段中缺少 file, url 或 path。")
            return None

        # 视频还可以带个封面，真是麻烦
        data = {"file": file_source}
        if napcat_type == NapcatSegType.video and seg.data.get("thumb"):
            data["thumb"] = seg.data.get("thumb")

        return {"type": napcat_type, "data": data}

    def _convert_record_seg(self, seg: Seg) -> dict[str, Any] | None:
        """处理语音消息，必须有 file 或 url."""
        return self._convert_media_seg(seg, NapcatSegType.record)

    def _convert_video_seg(self, seg: Seg) -> dict[str, Any] | None:
        """处理视频消息，必须有 file 或 url."""
        return self._convert_media_seg(seg, NapcatSegType.video)

    def _convert_file_seg(self, seg: Seg) -> dict[str, Any] | None:
        """处理文件消息，必须有 file 或 url."""
        return self._convert_media_seg(seg, "file")  # NapcatSegType里没定义，我直接写了

    def _convert_contact_seg(self, seg: Seg) -> dict[str, Any] | None:
        """处理联系人名片消息，必须有 contact_type 和 id."""
        contact_type = seg.data.get("contact_type")  # 'qq' or 'group'
        contact_id = seg.data.get("id")
        if not contact_type or not contact_id:
            logger.warning("发送联系人名片失败：Seg段中缺少 contact_type 或 id。")
            return None
        return {
            "type": NapcatSegType.contact,
            "data": {"type": contact_type, "id": str(contact_id)},
        }

    def _convert_music_seg(self, seg: Seg) -> dict[str, Any] | None:
        """处理音乐分享消息，支持平台音乐和自定义音乐."""
        music_type = seg.data.get("music_type")  # 'qq', '163', 'custom' etc.
        if not music_type:
            logger.warning("发送音乐分享失败：Seg段中缺少 music_type。")
            return None

        music_data = {}
        if music_type == "custom":
            # 自定义音乐需要 url, audio, title
            required_keys = ["url", "audio", "title"]
            if not all(key in seg.data for key in required_keys):
                logger.warning(f"发送自定义音乐失败：缺少必要字段 {required_keys}。")
                return None
            music_data = {
                "type": "custom",
                "url": seg.data["url"],
                "audio": seg.data["audio"],
                "title": seg.data["title"],
                "image": seg.data.get("image"),  # 可选
                "singer": seg.data.get("singer"),  # 可选
            }
        else:
            # 平台音乐需要 id
            music_id = seg.data.get("id")
            if not music_id:
                logger.warning(f"发送平台音乐({music_type})失败：缺少 id。")
                return None
            music_data = {"type": music_type, "id": str(music_id)}

        return {"type": NapcatSegType.music, "data": music_data}

    # --- 重构后的工具 ---
    async def _aicarus_segs_to_napcat_array(
        self, aicarus_segments: list[Seg]
    ) -> list[dict[str, Any]]:
        napcat_message_array: list[dict[str, Any]] = []
        for seg in aicarus_segments:
            if seg.type != "action_params":
                if converter := self.SEGMENT_CONVERTERS.get(seg.type):
                    if napcat_seg := converter(seg):
                        napcat_message_array.append(napcat_seg)
                else:
                    logger.warning(
                        f"发送处理器: 适配器不知道如何处理这个Seg类型: {seg.type}, 数据: {seg.data}"
                    )
        return napcat_message_array

    async def handle_aicarus_action(self, raw_aicarus_event_dict: dict) -> None:
        """处理来自AIcarus的动作事件."""
        try:
            aicarus_event = Event.from_dict(raw_aicarus_event_dict)
        except Exception as e:
            logger.error(f"发送处理器: 解析核心命令时出错: {e}", exc_info=True)
            return

        logger.info(
            f"发送处理器: 接收到动作事件 '{aicarus_event.event_id}'，"
            f"类型: {aicarus_event.event_type}"
        )

        success, message, details = await self._execute_action(aicarus_event)

        response_event = EventBuilder.create_action_response_event(
            response_type="success" if success else "failure",
            original_event=aicarus_event,
            message=message,
            data=details,
        )
        await recv_handler_aicarus.dispatch_to_core(response_event)
        logger.info(
            f"发送处理器: 已将动作 '{aicarus_event.event_id}' 的直接结果 "
            f"({'success' if success else 'failure'}) 发回核心"
        )

    async def _execute_action(self, event: Event) -> tuple[bool, str, dict[str, Any]]:
        """执行一个动作事件，返回执行结果."""
        full_action_type = event.event_type
        if not full_action_type.startswith("action."):
            error_msg = f"收到了一个非动作类型的事件: {full_action_type}"
            logger.warning(error_msg)
            return False, error_msg, {}

        # 提取动作别名，现在更加健壮
        action_alias = ".".join(full_action_type.split(".")[2:])
        logger.info(f"发送处理器正在分发动作，别名: '{action_alias}'")

        try:
            # 1. 消息发送是最高优待，特殊通道！因为它不使用 action_params。
            if action_alias == "send_message":
                return await self._handle_send_message_action(event)

            # 2. 找到承载着“神之旨意”的 action_params Seg
            params_seg = find_seg_by_type(event.content, "action_params")
            if not params_seg:
                # 某些无参数动作可能没有这个Seg，我们给它一个空的
                logger.debug(f"动作 '{action_alias}' 未提供 action_params，将使用空参数执行。")
                params = {}
            else:
                params = params_seg.data

            #  如果 params 里没有 group_id 或 user_id，就尝试从 event 的上下文中补全。
            if event.conversation_info:
                if "group_id" not in params and event.conversation_info.type == "group":
                    params["group_id"] = event.conversation_info.conversation_id
                if "user_id" not in params and event.conversation_info.type == "private":
                    params["user_id"] = event.conversation_info.conversation_id
            if event.user_info and "user_id" not in params:
                # 对于某些操作，比如 get_member_info，如果没指定目标，可能就是查自己
                pass  # 暂时不补全，避免歧义，但可以留个口子

            # 3. 接着是复杂的、需要特殊伺候的动作
            if action_alias in COMPLEX_ACTION_HANDLERS:
                handler = COMPLEX_ACTION_HANDLERS[action_alias]
                return await handler.execute(params, event, self)

            # 4. 最后，是那些可以一招秒杀的常规动作
            if action_alias in ACTION_MAPPING:
                api_func, required_params = ACTION_MAPPING[action_alias]

                # 检查贡品（必需参数）是否齐全
                if not all(key in params for key in required_params):
                    missing = [key for key in required_params if key not in params]
                    error_msg = f"动作 '{action_alias}' 失败：缺少必需参数: {missing}"
                    logger.warning(error_msg)
                    return False, error_msg, {}

                # 注入神力（server_connection），然后执行神之手！
                # 注意：我们只把 params 字典解包传进去，不多也不少
                response = await api_func(self.server_connection, **params)

                # 判断神谕的结果
                if response is not None:  # utils里的函数成功时返回字典，失败时返回None
                    return True, f"动作 '{action_alias}' 执行成功。", response
                else:
                    return (
                        False,
                        f"动作 '{action_alias}' 执行失败：Adapter API 返回错误或无响应。",
                        {},
                    )

            # 5. 如果在所有名录里都找不到
            error_msg = f"未知的动作别名 '{action_alias}'，适配器不知道该怎么做。"
            logger.warning(error_msg)
            return False, error_msg, {}

        except Exception as e:
            logger.error(f"执行动作 '{action_alias}' 时，发生异常: {e}", exc_info=True)
            return False, f"执行动作时出现异常: {e}", {}

    async def _handle_send_message_action(
        self, aicarus_event: Event
    ) -> tuple[bool, str, dict[str, Any]]:
        """专门处理发送消息的动作."""
        conv_info = aicarus_event.conversation_info
        if not conv_info:
            return False, "发送消息失败：缺少会话信息(conversation_info)。", {}

        target_group_id = (
            conv_info.conversation_id if conv_info and conv_info.type == "group" else None
        )
        target_user_id = (
            conv_info.conversation_id if conv_info and conv_info.type == "private" else None
        )

        if not target_group_id and not target_user_id:
            return False, "发送消息失败：缺少目标 group_id 或 user_id。", {}

        napcat_segments = await self._aicarus_segs_to_napcat_array(aicarus_event.content)
        if not napcat_segments:
            return False, "消息内容为空，无法发送。", {}

        params: dict[str, Any]
        napcat_action: str
        try:
            if target_group_id:
                napcat_action, params = (
                    "send_group_msg",
                    {"group_id": int(target_group_id), "message": napcat_segments},
                )
            elif target_user_id:
                # 检查是否为临时会话ID (格式: "用户ID.from.群ID")
                if ".from." in target_user_id:
                    logger.info(f"检测到临时会话ID: {target_user_id}，正在解析...")
                    parts = target_user_id.split(".from.")
                    if len(parts) == 2:
                        real_user_id, source_group_id = parts
                        # Napcat 发送临时会话消息也使用 send_private_msg,
                        # 但通常需要同时提供 user_id 和 group_id。
                        # 我们将解析出的两个ID都传进去。
                        params = {
                            "user_id": int(real_user_id),
                            "group_id": int(source_group_id),
                            "message": napcat_segments,
                        }
                        napcat_action = "send_private_msg"
                        logger.info(
                            f"解析成功, real_user_id: {real_user_id}, "
                            f"source_group_id: {source_group_id}"
                        )
                    else:
                        # 如果格式不正确，则返回错误
                        return False, f"临时会话ID格式错误: {target_user_id}", {}
                else:
                    # 如果不是临时会话，则按原逻辑处理
                    params = {
                        "user_id": int(target_user_id),
                        "message": napcat_segments,
                    }
                    napcat_action = "send_private_msg"
            else:
                return False, "没有找到发送目标", {}
        except (ValueError, TypeError):
            return (
                False,
                f"会话目标ID格式不对哦。当前ID: {target_group_id or target_user_id}",
                {},
            )

        response = await self._send_to_napcat_api(napcat_action, params)

        if response and response.get("status") == "ok":
            sent_message_id = str(response.get("data", {}).get("message_id", ""))

            if not sent_message_id:
                # 万一拿不到有效的 message_id，就直接报告失败，而不是去等待一个永远不会来的回声。
                # 理论上不会发生这种情况。
                err_msg = "Napcat API 响应成功，但未返回有效的 message_id。"
                logger.warning(f"动作 '{aicarus_event.event_id}' 失败: {err_msg}")
                return False, err_msg, {}

            # 我们将 Core 的 action_id 和 Napcat 的 message_id 关联起来
            # 这样可以在后续的回声确认中使用
            if sent_message_id and aicarus_event.event_id:
                pending_actions[sent_message_id] = aicarus_event.event_id
                logger.info(
                    f"动作 '{aicarus_event.event_id}' 已发送，"
                    f"其平台 message_id '{sent_message_id}' 已登记，等待回声确认。"
                )

            return True, "成功发送", {"sent_message_id": sent_message_id}
        else:
            err_msg = (
                response.get("message", "Napcat API 错误") if response else "Napcat 没有回应..."
            )
            return False, err_msg, {}

    async def _send_to_napcat_api(self, action: str, params: dict) -> dict | None:
        """将API请求安全地发送到Napcat服务器，并等待响应."""
        if not self.server_connection:
            logger.error(f"无法调用 Napcat API '{action}': WebSocket 连接不可用。")
            # 返回一个符合预期的错误结构
            return {
                "status": "error",
                "retcode": -1,
                "message": "Adapter not connected to Napcat",
            }

        request_uuid = str(uuid.uuid4())
        payload = {"action": action, "params": params, "echo": request_uuid}

        try:
            await self.server_connection.send(json.dumps(payload))
            # 使用 message_queue 中的工具等待响应
            return await get_napcat_api_response(request_uuid, timeout_seconds=30.0)
        except TimeoutError:
            logger.warning(f"调用 Napcat API '{action}' 超时。")
            return {"status": "error", "message": f"调用 Napcat API '{action}' 超时"}
        except websockets.ConnectionClosed:
            logger.error(f"调用 Napcat API '{action}' 时连接已关闭。")
            return {
                "status": "error",
                "retcode": -2,
                "message": "Connection closed while sending request",
            }
        except Exception as e:
            logger.error(f"发送请求到 Napcat 时发生未知错误: {e}", exc_info=True)
            return {"status": "error", "retcode": -3, "message": f"Unknown error: {e}"}


# 全局实例
send_handler_aicarus = SendHandlerAicarus()
