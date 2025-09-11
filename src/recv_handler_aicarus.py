# aicarus_napcat_adapter/src/recv_handler_aicarus.py
import asyncio
import time
import uuid
from typing import Any

import websockets

# 导入我们全新的协议对象
from aicarus_protocols import ConversationInfo, ConversationType, Event, Seg, UserInfo

from .config import get_config, global_config
from .event_definitions import get_event_handler

# 项目内部模块
from .logger import logger
from .napcat_definitions import NapcatSegType
from .qq_emoji_list import qq_face
from .utils import (
    napcat_get_forward_msg_content,
    napcat_get_group_info,
    napcat_get_member_info,
    napcat_get_self_info,
    process_image_url_to_aicarus_seg,
)


class RecvHandlerAicarus:
    """接收处理器，专门负责接收来自Napcat的消息，并将它们转换成AICarus能理解的格式."""

    router: Any = None
    server_connection: websockets.WebSocketServerProtocol | None = None
    napcat_bot_id: str | None = None
    global_config = global_config
    last_heart_beat: float = 0.0
    interval: float = 5.0

    def __init__(self) -> None:
        cfg = get_config()
        self.interval = cfg.napcat_heartbeat_interval_seconds

    async def process_event(self, napcat_event: dict) -> None:
        """唯一的任务，就是处理来自Napcat的事件."""
        post_type = napcat_event.get("post_type")
        handler = get_event_handler(post_type)
        if handler:
            await handler.execute(napcat_event, self)
        else:
            logger.warning(f"接收处理器: 不认识的事件类型 '{post_type}'，无法处理")

    # 获取Bot ID的任务，确保我们有自己的身份信息

    async def _get_bot_id(self) -> str | None:
        """获取我的ID，得不到就再试一次."""
        # 检查配置中是否强制指定了 Bot ID
        cfg = get_config()
        if cfg.force_self_id:
            forced_id = str(cfg.force_self_id).strip()
            if forced_id:
                self.napcat_bot_id = forced_id
                logger.info(f"已从配置中强制指定 Bot ID: {self.napcat_bot_id}")
                # 就是这里！通过 self.router 把爱（bot_id）注入到通信层！
                if self.router:
                    self.router.update_bot_id(self.napcat_bot_id)
                return self.napcat_bot_id

        if self.napcat_bot_id:
            return self.napcat_bot_id

        if not self.server_connection:
            logger.warning("无法获取 Bot ID：WebSocket 连接不可用。")
            return None

        max_retries = 3
        retry_delay_seconds = 5

        for attempt in range(max_retries):
            logger.info(f"尝试获取 Bot ID (第 {attempt + 1}/{max_retries} 次)...")
            self_info = await napcat_get_self_info(self.server_connection)
            if self_info and self_info.get("user_id"):
                self.napcat_bot_id = str(self_info.get("user_id"))
                logger.info(f"成功获取 Bot ID: {self.napcat_bot_id}")
                if self.router:
                    self.router.update_bot_id(self.napcat_bot_id)
                return self.napcat_bot_id

            logger.warning(
                f"获取 Bot ID 失败 (第 {attempt + 1}/{max_retries} 次)。API返回: {self_info}"
            )
            if attempt < max_retries - 1:
                logger.info(f"将在 {retry_delay_seconds} 秒后重试...")
                await asyncio.sleep(retry_delay_seconds)
            else:
                logger.error(f"已达到最大重试次数 ({max_retries})，仍未能获取 Bot ID。")

        return None

    async def _napcat_to_aicarus_userinfo(
        self, napcat_user_obj: dict, group_id: str | None = None
    ) -> UserInfo:
        """把Napcat的用户信息转换成AICarus的UserInfo对象."""
        user_id = str(napcat_user_obj.get("user_id", ""))
        nickname = napcat_user_obj.get("nickname")
        cardname = napcat_user_obj.get("card")
        permission_level = None
        role = None
        title = None

        if group_id and user_id and self.server_connection:
            member_data = await napcat_get_member_info(
                server_connection=self.server_connection,
                group_id=group_id,
                user_id=user_id,
            )

            if member_data:
                cardname = member_data.get("card") or cardname
                nickname = member_data.get("nickname") or nickname
                title = member_data.get("title")
                napcat_role = member_data.get("role")
                if napcat_role == "owner":
                    permission_level = "owner"
                    role = "owner"
                elif napcat_role == "admin":
                    permission_level = "admin"
                    role = "admin"
                else:
                    permission_level = "member"
                    role = "member"
        # 如果没有获取到角色信息，就使用默认值
        return UserInfo(
            user_id=user_id,
            user_nickname=nickname,
            user_cardname=cardname,
            user_titlename=title,
            permission_level=permission_level,
            role=role,
            additional_data=napcat_user_obj.get("additional_data", {}),
        )

    async def _napcat_to_aicarus_conversationinfo(
        self, napcat_group_id: str
    ) -> ConversationInfo | None:
        """把Napcat的群组ID转换成AICarus的ConversationInfo对象."""
        if not self.server_connection:
            return ConversationInfo(
                conversation_id=napcat_group_id,
                type=ConversationType.GROUP,
            )

        group_data = await napcat_get_group_info(
            server_connection=self.server_connection, group_id=napcat_group_id
        )
        group_name = group_data.get("group_name") if group_data else None
        return ConversationInfo(
            conversation_id=napcat_group_id,
            type=ConversationType.GROUP,
            name=group_name,
        )

    async def _napcat_to_aicarus_private_conversationinfo(
        self, napcat_user_info: UserInfo
    ) -> ConversationInfo | None:
        """把私聊的用户包装成一个独立的ConversationInfo对象."""
        if not napcat_user_info or not napcat_user_info.user_id:
            return None
        return ConversationInfo(
            conversation_id=napcat_user_info.user_id,
            type=ConversationType.PRIVATE,
            name=napcat_user_info.user_nickname,
        )

    async def _napcat_to_aicarus_seglist(
        self, napcat_segments: list[dict[str, Any]], napcat_event: dict
    ) -> list[Seg]:
        """把Napcat的消息段转换成AICarus能理解的格式。."""
        aicarus_segs: list[Seg] = []
        for seg in napcat_segments:
            seg_type = seg.get("type")
            seg_data = seg.get("data", {})
            aicarus_s: Seg | None = None

            if seg_type == NapcatSegType.text:
                aicarus_s = Seg(type="text", data={"text": seg_data.get("text", "")})

            elif seg_type == NapcatSegType.face:
                face_id = seg_data.get("id")
                face_name = qq_face.get(face_id)
                aicarus_s = Seg(type="face", data={"id": face_id, "name": face_name})

            # [MODIFIED] 统一图片处理逻辑
            elif seg_type == NapcatSegType.image:
                image_url = seg_data.get("url")
                file_id = seg_data.get("file")

                if not image_url:
                    # 如果没有URL，只能返回一个包含file_id的基础图片Seg
                    aicarus_s = Seg(type="image", data={"file_id": file_id, "summary": "image"})
                else:
                    # 如果有URL，交给全能处理器来决策返回什么类型的Seg
                    aicarus_s = await process_image_url_to_aicarus_seg(image_url, file_id)

            elif seg_type == NapcatSegType.at:
                qq_num = seg_data.get("qq")
                # 优先使用Napcat提供的name，因为它可能包含更准确的群昵称
                display_name = seg_data.get("name")
                if not display_name:
                    display_name = f"@{qq_num}" if qq_num and str(qq_num) != "all" else "@全体成员"
                else:
                    # 确保它总是以@开头
                    if not display_name.startswith("@"):
                        display_name = f"@{display_name}"

                aicarus_s = Seg(
                    type="at",
                    data={
                        "user_id": str(qq_num) if qq_num else "",
                        "display_name": display_name,
                    },
                )

            elif seg_type == NapcatSegType.reply:
                # 不仅要知道回复了哪条消息，还要知道是谁发的，说了啥
                quote_info = seg_data  # 在Napcat中，reply seg的data就是引用信息的全部
                aicarus_s = Seg(
                    type="quote",  # 我决定用 "quote" 这个更明确的类型
                    data={
                        "message_id": quote_info.get("id"),
                        "user_id": str(quote_info.get("qq")) if quote_info.get("qq") else None,
                        "nickname": quote_info.get("name"),
                        "content": quote_info.get("text"),  # 被引用的内容摘要
                        "time": quote_info.get("time"),
                    },
                )

            elif seg_type == NapcatSegType.record:
                aicarus_s = Seg(
                    type="record",
                    data={"file": seg_data.get("file"), "url": seg_data.get("url")},
                )

            elif seg_type == NapcatSegType.video:
                aicarus_s = Seg(
                    type="video",
                    data={"file": seg_data.get("file"), "url": seg_data.get("url")},
                )

            elif seg_type == NapcatSegType.forward:
                forward_id = seg_data.get("id")
                forward_content = None
                if forward_id and self.server_connection:
                    try:
                        # 把合并消息的内容获取出来
                        forward_content = await napcat_get_forward_msg_content(
                            self.server_connection, forward_id
                        )
                    except Exception as e:
                        logger.warning(f"获取合并转发内容失败了啦: {e}")

                if forward_content:
                    aicarus_s = Seg(
                        type="forward",
                        data={"id": forward_id, "content": forward_content},
                    )
                else:
                    aicarus_s = Seg(type="text", data={"text": "[合并转发消息(获取失败)]"})

            elif seg_type == NapcatSegType.json:
                aicarus_s = Seg(type="json_card", data={"content": seg_data.get("data", "{}")})

            elif seg_type == NapcatSegType.xml:
                aicarus_s = Seg(type="xml_card", data={"content": seg_data.get("data", "")})

            elif seg_type == NapcatSegType.share:
                aicarus_s = Seg(
                    type="share",
                    data={
                        "url": seg_data.get("url", ""),
                        "title": seg_data.get("title", ""),
                        "content": seg_data.get("content", ""),
                        "image_url": seg_data.get("image", ""),
                    },
                )

            elif seg_type == NapcatSegType.music:
                # 哼，收到音乐分享，直接把数据丢过去就行了，简单。
                # 你的核心那边自己看 seg_data 里面的 'type' 是 'qq' 还是 'custom'
                aicarus_s = Seg(type="music", data=seg_data)

            elif seg_type == NapcatSegType.contact:
                # 推荐联系人也一样，我帮你把字段名对齐一下
                aicarus_s = Seg(
                    type="contact",
                    data={
                        "contact_type": seg_data.get("type"),  # 'qq' or 'group'
                        "id": str(seg_data.get("id")),
                    },
                )

            elif seg_type == NapcatSegType.file:
                # 文件也一样，把 Napcat 给的所有信息都塞给你，你自己看着办吧。
                aicarus_s = Seg(type="file", data=seg_data)

            elif seg_type == NapcatSegType.location:
                # 位置信息，什么经纬度、标题，都给你。
                aicarus_s = Seg(type="location", data=seg_data)

            else:
                # 就算是未知的类型，也会特别标记出来
                logger.warning(f"不认识的Napcat消息体: {seg_type}，数据: {seg_data}")
                aicarus_s = Seg(
                    type="unknown",
                    data={"napcat_type": seg_type, "napcat_data": seg_data},
                )

            if aicarus_s:
                aicarus_segs.append(aicarus_s)
        return aicarus_segs

    async def check_heartbeat(self, bot_id: str) -> None:
        """定期检查心跳，确保连接活跃."""
        while True:
            await asyncio.sleep(self.interval)
            if self.server_connection and time.time() - self.last_heart_beat > self.interval + 5:
                logger.warning("连接似乎已经失去心跳了，准备断开连接...")

                platform_id = self.global_config.core_platform_id
                disconnect_event_type = f"meta.{platform_id}.lifecycle.disconnect"

                disconnect_seg = Seg(
                    type="meta.lifecycle.disconnect",
                    data={"reason": "heartbeat_timeout", "adapter_version": "2.0.0"},
                )
                disconnect_event = Event(
                    event_id=f"meta_disconnect_{uuid.uuid4()}",
                    event_type=disconnect_event_type,
                    time=time.time() * 1000.0,
                    bot_id=bot_id,
                    user_info=None,
                    conversation_info=None,
                    content=[disconnect_seg],
                )
                await self.dispatch_to_core(disconnect_event)
                break
            else:
                logger.debug(f"心跳正常 ({bot_id})")

    async def dispatch_to_core(self, event: Event) -> None:
        """将事件发送到核心处理器."""
        if self.router:
            logger.info(f"发送 -> {event.event_type} (ID: {event.event_id})")
            await self.router.send_event_to_core(event.to_dict())
        else:
            logger.error("接收处理器没有配置路由器，无法发送事件到核心。请检查初始化。")


# 全局实例
recv_handler_aicarus = RecvHandlerAicarus()
