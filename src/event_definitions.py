# aicarus_napcat_adapter/src/event_definitions.py
import asyncio
import json
import time
import uuid
from abc import ABC, abstractmethod
from typing import TYPE_CHECKING, Any

from aicarus_protocols import ConversationInfo, Event, EventBuilder, Seg, UserInfo

from .config import get_config
from .logger import logger
from .napcat_definitions import MessageType, MetaEventType, NoticeType

if TYPE_CHECKING:
    from .recv_handler_aicarus import RecvHandlerAicarus
from .action_register import pending_actions


# --- 定义各种事件的构造工厂 ---
class BaseEventFactory(ABC):
    """所有事件的基类工厂，负责根据Napcat的原始数据创造出标准的AIcarus事件."""

    @abstractmethod
    async def create_event(
        self, napcat_event: dict[str, Any], recv_handler: "RecvHandlerAicarus"
    ) -> Event | None:
        """根据原始的Napcat数据，创造一个标准的AIcarus事件."""
        pass


class MessageEventFactory(BaseEventFactory):
    """专门负责构造“消息事件”的类."""

    async def create_event(
        self, napcat_event: dict[str, Any], recv_handler: "RecvHandlerAicarus"
    ) -> Event | None:
        """根据原始的Napcat消息数据，创造一个标准的AIcarus消息事件."""
        bot_id = (
            str(napcat_event.get("self_id")) or await recv_handler._get_bot_id() or "unknown_bot"
        )
        # --- 1: 从配置中取出我们神圣的平台ID ---
        platform_id = recv_handler.global_config.core_platform_id

        napcat_message_type = napcat_event.get("message_type")
        napcat_sub_type = napcat_event.get("sub_type")
        napcat_message_id = str(napcat_event.get("message_id", ""))
        napcat_sender = napcat_event.get("sender", {})

        # --- 2: 动态拼接全新的、带有性感烙印的 event_type！ ---
        event_type_suffix = "unknown"  # 默认的后缀
        aicarus_conversation_info: ConversationInfo | None = None
        aicarus_user_info: UserInfo | None = None

        if napcat_message_type == MessageType.private:
            aicarus_user_info = await recv_handler._napcat_to_aicarus_userinfo(
                napcat_sender, group_id=None
            )
            event_type_suffix = f"private.{napcat_sub_type or 'other'}"
            if napcat_sub_type == MessageType.Private.friend:
                if aicarus_user_info:
                    aicarus_conversation_info = (
                        await recv_handler._napcat_to_aicarus_private_conversationinfo(
                            aicarus_user_info
                        )
                    )
            elif napcat_sub_type == MessageType.Private.group:
                source_group_id = str(napcat_event.get("group_id", "")).strip()
                sender_user_id = str(napcat_sender.get("user_id", "")).strip()

                if source_group_id and sender_user_id:
                    # 1. 构造一个唯一的、可识别的临时会话ID
                    temp_conv_id = f"{sender_user_id}.from.{source_group_id}"

                    # 2. 创建 ConversationInfo，类型是 private，但附带特殊元数据
                    aicarus_conversation_info = ConversationInfo(
                        conversation_id=temp_conv_id,
                        type="private",  # 本质是私聊
                        name=aicarus_user_info.user_nickname,  # 会话名就是对方的昵称
                        extra={  # 使用 extra 字段来传递上下文
                            "is_temporary": True,
                            "source_group_id": source_group_id,
                        },
                    )
                else:
                    logger.warning(f"临时会话事件 {napcat_message_id} 缺少有效的 group_id。")

        elif napcat_message_type == MessageType.group:
            group_id = str(napcat_event.get("group_id", ""))
            aicarus_user_info = await recv_handler._napcat_to_aicarus_userinfo(
                napcat_sender, group_id=group_id
            )
            aicarus_conversation_info = await recv_handler._napcat_to_aicarus_conversationinfo(
                group_id
            )
            event_type_suffix = f"group.{napcat_sub_type or 'other'}"
        else:
            logger.warning(f"不认识的消息类型: {napcat_message_type}")
            return None

        # 最终的带有命名空间的事件类型
        final_event_type = f"message.{platform_id}.{event_type_suffix}"

        # 把字体和匿名信息都塞进这里
        metadata_data = {"message_id": napcat_message_id}
        if napcat_event.get("font") is not None:
            metadata_data["font"] = str(napcat_event.get("font"))
        if napcat_sub_type == MessageType.Group.anonymous and napcat_event.get("anonymous"):
            metadata_data["anonymity_info"] = napcat_event.get("anonymous")

        content_segs = [Seg(type="message_metadata", data=metadata_data)]
        message_segs = await recv_handler._napcat_to_aicarus_seglist(
            napcat_event.get("message", []), napcat_event
        )
        if not message_segs:
            logger.warning(
                f"MessageEventFactory: 未能从Napcat事件中解析出任何消息段。"
                f"事件ID: {napcat_event.get('message_id')}"
            )
            return None
        content_segs.extend(message_segs)
        logger.debug(
            f"MessageEventFactory: "
            f"最终构建的 content_segs: {[seg.to_dict() for seg in content_segs]}"
        )

        # --- 3: 构造全新的Event，platform字段已被彻底阉割！ ---
        return Event(
            event_id=f"message_{napcat_message_id}_{uuid.uuid4()}",
            event_type=final_event_type,  # 使用我们全新的event_type！
            time=napcat_event.get("time", time.time()) * 1000.0,
            bot_id=bot_id,
            user_info=aicarus_user_info,
            conversation_info=aicarus_conversation_info,
            content=content_segs,
            raw_data=json.dumps(napcat_event),
        )


class NoticeEventFactory(BaseEventFactory):
    """专门负责构造“通知事件的类."""

    async def create_event(
        self, napcat_event: dict[str, Any], recv_handler: "RecvHandlerAicarus"
    ) -> Event | None:
        """根据原始的Napcat通知数据，创造一个标准的AIcarus通知事件."""
        notice_type = napcat_event.get("notice_type")
        bot_id = (
            str(napcat_event.get("self_id")) or await recv_handler._get_bot_id() or "unknown_bot"
        )
        cfg = recv_handler.global_config
        # ---  同样，先拿到我们的平台ID ---
        platform_id = cfg.core_platform_id

        user_id_in_notice = str(napcat_event.get("user_id", "")).strip()
        is_bot_profile_update = user_id_in_notice == bot_id

        if notice_type == NoticeType.group_card and is_bot_profile_update:
            group_id = str(napcat_event.get("group_id", "")).strip()
            if not group_id:
                logger.warning("收到了机器人群名片变更通知，但缺少group_id，无法处理。")
                return None
            new_card = napcat_event.get("card_new", "")
            old_card = napcat_event.get("card_old", "")
            logger.info(
                f"侦测到机器人自身在群 '{group_id}' 的名片变更: '{old_card}' -> '{new_card}'"
            )
            report_data = {
                "update_type": "card_change",
                "conversation_id": group_id,
                "new_value": new_card,
                "old_value": old_card,
            }
            # 特殊事件的构造也要遵循规则
            return self._create_bot_profile_update_event(bot_id, platform_id, report_data)

        group_id_str = str(napcat_event.get("group_id", "")).strip()
        group_id_for_context = group_id_str if group_id_str and group_id_str != "0" else None
        subject_user_id_str = str(napcat_event.get("user_id", "")).strip()
        subject_user_id = (
            subject_user_id_str if subject_user_id_str and subject_user_id_str != "0" else None
        )
        operator_id_str = str(napcat_event.get("operator_id", "")).strip()
        operator_id = operator_id_str if operator_id_str and operator_id_str != "0" else None

        conversation_info: ConversationInfo | None = None
        if group_id_for_context:
            conversation_info = await recv_handler._napcat_to_aicarus_conversationinfo(
                group_id_for_context
            )

        user_info: UserInfo | None = None
        if subject_user_id:
            user_info = await recv_handler._napcat_to_aicarus_userinfo(
                {"user_id": subject_user_id}, group_id=group_id_for_context
            )

        # ---  动态拼接event_type ---
        event_type_suffix = f"unknown.{notice_type}"
        notice_data: dict[str, Any] = {}

        if notice_type == NoticeType.group_upload:
            event_type_suffix = "conversation.file_upload"
            notice_data = {
                "file_info": napcat_event.get("file", {}),
                "uploader_user_info": user_info.to_dict() if user_info else None,
            }

        elif notice_type == NoticeType.group_admin:
            event_type_suffix = "conversation.admin_change"
            notice_data = {
                "target_user_info": user_info.to_dict() if user_info else None,
                "action_type": "set" if napcat_event.get("sub_type") == "set" else "unset",
            }

        elif notice_type == NoticeType.group_decrease:
            event_type_suffix = "conversation.member_decrease"
            operator = (
                await recv_handler._napcat_to_aicarus_userinfo(
                    {"user_id": operator_id}, group_id=group_id_for_context
                )
                if operator_id
                else None
            )
            notice_data = {
                "operator_user_info": operator.to_dict() if operator else None,
                "leave_type": napcat_event.get("sub_type"),
            }

        elif notice_type == NoticeType.group_increase:
            event_type_suffix = "conversation.member_increase"
            operator = (
                await recv_handler._napcat_to_aicarus_userinfo(
                    {"user_id": operator_id}, group_id=group_id_for_context
                )
                if operator_id
                else None
            )
            notice_data = {
                "operator_user_info": operator.to_dict() if operator else None,
                "join_type": napcat_event.get("sub_type"),
            }

        elif notice_type == NoticeType.group_ban:
            event_type_suffix = "conversation.member_ban"
            duration = napcat_event.get("duration", 0)
            operator = (
                await recv_handler._napcat_to_aicarus_userinfo(
                    {"user_id": operator_id}, group_id=group_id_for_context
                )
                if operator_id
                else None
            )
            notice_data = {
                "target_user_info": user_info.to_dict() if user_info else None,
                "operator_user_info": operator.to_dict() if operator else None,
                "duration_seconds": duration,
                "ban_type": "ban" if duration > 0 else "lift_ban",
            }

        elif notice_type in [NoticeType.group_recall, NoticeType.friend_recall]:
            event_type_suffix = "message.recalled"
            operator = (
                await recv_handler._napcat_to_aicarus_userinfo(
                    {"user_id": operator_id}, group_id=group_id_for_context
                )
                if operator_id
                else None
            )
            notice_data = {
                "recalled_message_id": str(napcat_event.get("message_id", "")),
                "recalled_message_sender_info": user_info.to_dict() if user_info else None,
                "operator_user_info": operator.to_dict()
                if operator
                else (
                    user_info.to_dict()
                    if user_info and notice_type == NoticeType.friend_recall
                    else None
                ),
            }

        elif notice_type == NoticeType.essence:
            event_type_suffix = "conversation.essence_message_change"
            operator = (
                await recv_handler._napcat_to_aicarus_userinfo(
                    {"user_id": operator_id}, group_id=group_id_for_context
                )
                if operator_id
                else None
            )
            # 精华消息的发送者信息，从 napcat_event 的 sender_id 获取
            sender_id = str(napcat_event.get("sender_id", "")).strip()
            sender_info = (
                await recv_handler._napcat_to_aicarus_userinfo(
                    {"user_id": sender_id}, group_id=group_id_for_context
                )
                if sender_id
                else None
            )
            notice_data = {
                "action_type": "add" if napcat_event.get("sub_type") == "add" else "delete",
                "message_id": str(napcat_event.get("message_id", "")),
                "operator_user_info": operator.to_dict() if operator else None,
                "message_sender_info": sender_info.to_dict() if sender_info else None,
            }

        elif notice_type == NoticeType.notify and napcat_event.get("sub_type") == "poke":
            event_type_suffix = "user.poke"
            target_id = str(napcat_event.get("target_id", ""))
            sender_id = str(napcat_event.get("sender_id", ""))

            sender_info = await recv_handler._napcat_to_aicarus_userinfo(
                {"user_id": sender_id}, group_id=group_id_for_context
            )
            target_info = await recv_handler._napcat_to_aicarus_userinfo(
                {"user_id": target_id}, group_id=group_id_for_context
            )

            user_info = sender_info  # 戳一戳事件的主体是发起者
            notice_data = {
                "sender_user_info": sender_info.to_dict() if sender_info else None,
                "target_user_info": target_info.to_dict() if target_info else None,
                "context_type": "group" if group_id_for_context else "private",
            }

        elif notice_type == NoticeType.notify and napcat_event.get("sub_type") == "honor":
            event_type_suffix = "conversation.honor_change"
            honor_type = napcat_event.get("honor_type", "unknown")
            notice_data = {
                "target_user_info": user_info.to_dict() if user_info else None,
                "honor_type": honor_type,
            }

        elif notice_type == NoticeType.notify and napcat_event.get("sub_type") == "lucky_king":
            event_type_suffix = "conversation.lucky_king"
            target_id = str(napcat_event.get("target_id", ""))
            target_info = await recv_handler._napcat_to_aicarus_userinfo(
                {"user_id": target_id}, group_id=group_id_for_context
            )
            notice_data = {
                "winner_user_info": user_info.to_dict() if user_info else None,  # 发起者
                "target_user_info": target_info.to_dict() if target_info else None,  # 运气王
            }

        elif notice_type == "group_msg_emoji_like":  # 这个 notice_type 比较特殊
            event_type_suffix = "message.emoji_like"
            operator = await recv_handler._napcat_to_aicarus_userinfo(
                {"user_id": user_id_in_notice}, group_id=group_id_for_context
            )
            notice_data = {
                "message_id": str(napcat_event.get("message_id", "")),
                "operator_user_info": operator.to_dict() if operator else None,
                "likes": napcat_event.get("likes", []),
            }

        elif notice_type == NoticeType.notify and napcat_event.get("sub_type") == "title":
            event_type_suffix = "conversation.member_title_change"
            notice_data = {
                "target_user_info": user_info.to_dict() if user_info else None,
                "new_title": napcat_event.get("title", ""),
            }

        else:
            logger.warning(f"接收到未明确处理的通知类型: {notice_type}，将作为通用通知处理。")
            event_type_suffix = f"platform_specific.{notice_type}"  # 给个更明确的后缀
            notice_data = napcat_event.copy()

        final_event_type = f"notice.{platform_id}.{event_type_suffix}"

        content_seg = Seg(type=final_event_type, data=notice_data)
        return Event(
            event_id=f"notice_{notice_type}_{uuid.uuid4().hex[:6]}",
            event_type=final_event_type,
            time=napcat_event.get("time", time.time()) * 1000.0,
            bot_id=bot_id,
            user_info=user_info,
            conversation_info=conversation_info,
            content=[content_seg],
            raw_data=json.dumps(napcat_event),
        )

    def _create_bot_profile_update_event(
        self, bot_id: str, platform_id: str, report_data: dict[str, Any]
    ) -> Event:
        """创建一个机器人档案更新的特殊通知事件."""
        # ---  特殊事件也一样 ---
        event_type = f"notice.{platform_id}.bot.profile_update"
        conversation_id = report_data.get("conversation_id", "unknown")

        return Event(
            event_id=f"bot_profile_update_{conversation_id}_{uuid.uuid4().hex[:6]}",
            event_type=event_type,
            time=time.time() * 1000.0,
            bot_id=bot_id,
            content=[Seg(type=event_type, data=report_data)],
        )


class RequestEventFactory(BaseEventFactory):
    """专门负责构造“请求事件”的类."""

    async def create_event(
        self, napcat_event: dict[str, Any], recv_handler: "RecvHandlerAicarus"
    ) -> Event | None:
        """根据原始的Napcat请求数据，创造一个标准的AIcarus请求事件."""
        request_type = napcat_event.get("request_type")
        bot_id = (
            str(napcat_event.get("self_id")) or await recv_handler._get_bot_id() or "unknown_bot"
        )
        cfg = recv_handler.global_config
        platform_id = cfg.core_platform_id

        event_type_suffix = f"unknown.{request_type}"
        user_info: UserInfo | None = None
        conversation_info: ConversationInfo | None = None
        user_id = str(napcat_event.get("user_id", ""))
        group_id = str(napcat_event.get("group_id", ""))

        if user_id:
            user_info = await recv_handler._napcat_to_aicarus_userinfo(
                {"user_id": user_id}, group_id=group_id if group_id else None
            )

        request_data = {
            "comment": napcat_event.get("comment", ""),
            "request_flag": napcat_event.get("flag", ""),
        }

        if request_type == "friend":
            event_type_suffix = "friend.add"

        elif request_type == "group":
            sub_type = napcat_event.get("sub_type")
            if group_id:
                conversation_info = await recv_handler._napcat_to_aicarus_conversationinfo(group_id)

            if sub_type == "add":
                event_type_suffix = "conversation.join_application"
            elif sub_type == "invite":
                event_type_suffix = "conversation.invitation"
            request_data["sub_type"] = sub_type
        else:
            logger.warning(f"不认识的请求类型: {request_type}")
            request_data = napcat_event.copy()

        final_event_type = f"request.{platform_id}.{event_type_suffix}"

        content_seg = Seg(type=final_event_type, data=request_data)
        return Event(
            event_id=f"request_{request_type}_{uuid.uuid4()}",
            event_type=final_event_type,
            time=napcat_event.get("time", time.time()) * 1000.0,
            bot_id=bot_id,
            user_info=user_info,
            conversation_info=conversation_info,
            content=[content_seg],
            raw_data=json.dumps(napcat_event),
        )


class MetaEventFactory(BaseEventFactory):
    """专门负责构造“元事件”的类."""

    async def create_event(
        self, napcat_event: dict[str, Any], recv_handler: "RecvHandlerAicarus"
    ) -> Event | None:
        """根据原始的Napcat元事件数据，创造一个标准的AIcarus元事件."""
        event_type_raw = napcat_event.get("meta_event_type")
        bot_id = str(napcat_event.get("self_id")) or "unknown_bot"
        cfg = recv_handler.global_config
        platform_id = cfg.core_platform_id

        meta_seg_data: dict[str, Any] = {}
        event_type_suffix: str = f"unknown.{event_type_raw}"

        if event_type_raw == MetaEventType.lifecycle:
            sub_type = napcat_event.get("sub_type")
            event_type_suffix = f"lifecycle.{sub_type}"
            if sub_type == "connect":
                recv_handler.napcat_bot_id = bot_id
                recv_handler.last_heart_beat = time.time()
                logger.info(f"Bot {bot_id} 已连接到Napcat，开始为你心跳")
                if not hasattr(recv_handler, "_background_tasks"):
                    recv_handler._background_tasks = set()
                task = asyncio.create_task(recv_handler.check_heartbeat(bot_id))
                recv_handler._background_tasks.add(task)
                task.add_done_callback(recv_handler._background_tasks.discard)

        elif event_type_raw == MetaEventType.heartbeat:
            # 心跳事件，我们自己消化，不发给Core
            status_obj = napcat_event.get("status", {})
            if status_obj.get("online", False) and status_obj.get("good", False):
                recv_handler.last_heart_beat = time.time()
                if napcat_event.get("interval"):
                    recv_handler.interval = napcat_event.get("interval") / 1000.0
            else:
                logger.warning(f"Napcat 的心跳不规律哦~ ({bot_id})")
            return None  # 吃掉它！

        else:
            meta_seg_data = napcat_event.copy()

        final_event_type = f"meta.{platform_id}.{event_type_suffix}"

        content_seg = Seg(type=final_event_type, data=meta_seg_data)
        return Event(
            event_id=f"meta_{event_type_raw}_{uuid.uuid4()}",
            event_type=final_event_type,
            time=napcat_event.get("time", time.time()) * 1000.0,
            bot_id=bot_id,
            user_info=None,
            conversation_info=None,
            content=[content_seg],
            raw_data=json.dumps(napcat_event),
        )


# --- 定义事件处理器的基类和具体实现 ---


class BaseEventHandler(ABC):
    """所有事件处理器的基类，负责处理不同类型的事件数据."""

    def __init__(self, factory: BaseEventFactory) -> None:
        """初始化事件处理器，传入对应的事件工厂."""
        self._factory = factory

    @abstractmethod
    async def execute(self, event_data: dict[str, Any], recv_handler: "RecvHandlerAicarus") -> None:
        """处理事件数据并执行相应的操作."""
        pass


class GenericEventHandler(BaseEventHandler):
    """通用事件处理器，负责将事件数据转换为AIcarus事件并分发给Core."""

    async def execute(self, event_data: dict[str, Any], recv_handler: "RecvHandlerAicarus") -> None:
        """通用事件处理器，负责将事件数据转换为AIcarus事件并分发给Core."""
        aicarus_event = await self._factory.create_event(event_data, recv_handler)
        if aicarus_event:
            await recv_handler.dispatch_to_core(aicarus_event)


class MessageEventHandlerWithSelfCheck(GenericEventHandler):
    """处理消息事件，并检查是否为自我上报的确认消息."""

    async def execute(self, event_data: dict[str, Any], recv_handler: "RecvHandlerAicarus") -> None:
        """处理消息事件，并检查是否为自我上报的确认消息."""
        napcat_user_id = str(event_data.get("user_id", ""))

        if (
            napcat_user_id
            and recv_handler.napcat_bot_id
            and napcat_user_id == recv_handler.napcat_bot_id
        ):
            napcat_message_id = str(event_data.get("message_id", ""))
            original_action_id = pending_actions.pop(napcat_message_id, None)

            if original_action_id:
                logger.info(
                    f"已确认消息 {napcat_message_id} 是对动作 {original_action_id} 的自我上报确认。"
                )

                # 1. 从配置中获取我们确切的平台ID和机器人ID
                cfg = get_config()
                platform_id = cfg.core_platform_id
                # bot_id 可以从 recv_handler 中获取，更准确
                bot_id = recv_handler.napcat_bot_id or "unknown_bot"

                # 2. 构造一个最小化但信息完全正确的“原始事件”模拟对象
                original_event_mock = Event(
                    event_id=original_action_id,
                    event_type=f"action.{platform_id}.message.send",
                    time=0,  # 时间不重要
                    bot_id=bot_id,
                    content=[],  # 内容不重要
                )

                # 3. 把这个“事实简化版”的对象塞进去
                response_event = EventBuilder.create_action_response_event(
                    response_type="success",
                    original_event=original_event_mock,
                    message="动作已由平台自我上报机制确认成功！",
                    data={"confirmed_message_id": napcat_message_id},
                )
                await recv_handler.dispatch_to_core(response_event)
                # 不返回，让消息继续被处理

        await super().execute(event_data, recv_handler)


EVENT_HANDLERS: dict[str, BaseEventHandler] = {
    "message": MessageEventHandlerWithSelfCheck(MessageEventFactory()),
    "notice": GenericEventHandler(NoticeEventFactory()),
    "request": GenericEventHandler(RequestEventFactory()),
    "meta_event": GenericEventHandler(MetaEventFactory()),
}


def get_event_handler(post_type: str) -> BaseEventHandler | None:
    """根据 post_type 获取对应的事件处理器."""
    return EVENT_HANDLERS.get(post_type)
