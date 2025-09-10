# aicarus_napcat_adapter/src/action_definitions.py
from collections.abc import Awaitable, Callable
from typing import TYPE_CHECKING, Any

from aicarus_protocols import Event, Seg

from . import utils

if TYPE_CHECKING:
    from .send_handler_aicarus import SendHandlerAicarus


# ==============================================================================
# 1. 复杂动作处理器 (Complex Action Handlers)
# ==============================================================================
# 对于那些需要特殊逻辑处理的动作，我们定义了一个基类和一些具体的处理器。
# 比如'send_forward_message'，它需要解析整个 event.content，很麻烦，得特殊对待。
# ------------------------------------------------------------------------------


class BaseComplexActionHandler:
    """复杂动作的基类，给它们一个统一的接口."""

    @staticmethod
    async def execute(
        params: dict[str, Any], event: Event, send_handler: "SendHandlerAicarus"
    ) -> tuple[bool, str, dict[str, Any]]:
        """执行复杂动作的逻辑."""
        raise NotImplementedError


class SendForwardMessageHandler(BaseComplexActionHandler):
    """处理合并转发消息的特殊逻辑."""

    @staticmethod
    async def execute(
        params: dict[str, Any], event: Event, send_handler: "SendHandlerAicarus"
    ) -> tuple[bool, str, dict[str, Any]]:
        """执行合并转发消息的逻辑."""
        # 合并转发的核心是 event.content 里的 'node' 列表，而不是 action_params
        nodes = [seg for seg in event.content if seg.type == "node"]
        if not nodes:
            return False, "发送合并转发失败：内容中必须包含 'node' 消息段。", {}

        # 转换所有节点
        napcat_nodes = []
        for node_seg in nodes:
            node_data = node_seg.data
            # 伪造的消息节点需要 user_id 和 nickname (Napcat 叫 uin 和 name)
            if "uin" in node_data and "name" in node_data:
                # 节点里的内容也得转换成Napcat格式
                napcat_content = await send_handler._aicarus_segs_to_napcat_array(
                    [Seg.from_dict(c) for c in node_data.get("content", [])]
                )
                napcat_nodes.append(
                    {
                        "user_id": str(node_data["uin"]),
                        "nickname": str(node_data["name"]),
                        "content": napcat_content,
                    }
                )
            # 真实消息转发只需要 message_id (Napcat 叫 id)
            elif "id" in node_data:
                napcat_nodes.append({"id": str(node_data["id"])})
            else:
                return (
                    False,
                    "发送合并转发失败：节点缺少必要字段 ('id' 或 'uin'/'name')。",
                    {},
                )

        # 确定是发给群还是私聊，现在从 params 里拿，多干净！
        target_group_id = params.get("group_id")
        target_user_id = params.get("user_id")

        api_params: dict[str, Any]
        napcat_action: str = "send_forward_msg"
        try:
            if target_group_id:
                api_params = {
                    "group_id": int(target_group_id),
                    "messages": napcat_nodes,
                }
            elif target_user_id:
                api_params = {"user_id": int(target_user_id), "messages": napcat_nodes}
            else:
                return (
                    False,
                    "发送合并转发失败：缺少会话目标 (group_id 或 user_id)。",
                    {},
                )
        except (ValueError, TypeError):
            return (
                False,
                f"会话目标ID格式不对哦。当前ID: {target_group_id or target_user_id}",
                {},
            )

        # 发送给Napcat
        response = await send_handler._send_to_napcat_api(napcat_action, api_params)

        if response and response.get("status") == "ok":
            sent_message_id = str(response.get("data", {}).get("message_id", ""))
            forward_id = str(
                response.get("data", {}).get("forward_id", "")
                or response.get("data", {}).get("res_id", "")
            )
            return (
                True,
                "合并转发消息已发送。",
                {"sent_message_id": sent_message_id, "forward_id": forward_id},
            )
        else:
            err_msg = (
                response.get("message", "Napcat API 错误") if response else "Napcat 没有回应我..."
            )
            return False, err_msg, {}


class GetBotProfileHandler(BaseComplexActionHandler):
    """处理获取机器人自身完整档案的特殊逻辑."""

    @staticmethod
    async def execute(
        params: dict[str, Any], event: Event, send_handler: "SendHandlerAicarus"
    ) -> tuple[bool, str, dict[str, Any]]:
        """执行获取机器人档案的逻辑.

        这个动作现在会返回一个更完整的报告，包括：
        - 机器人自己的 user_id 和 nickname
        - 它所在的平台 platform
        - 它加入的所有群聊的列表，以及它在每个群的群名片等信息
        """
        print("正在执行复杂的 get_bot_profile 动作，将返回完整档案...")
        connection = send_handler.server_connection
        if not connection:
            return False, "适配器未连接到Napcat，无法获取档案。", {}

        # 1. 获取机器人自身基础信息
        self_info = await utils.napcat_get_self_info(connection)
        if not self_info or not self_info.get("user_id"):
            return False, "无法获取机器人自身的基础信息。", {}

        bot_id = str(self_info["user_id"])
        bot_nickname = self_info.get("nickname")

        # 2. 获取机器人所在的群列表
        group_list = await utils.napcat_get_list(connection, list_type="group")
        if group_list is None:  # get_list 失败返回 None
            print("无法获取群列表，档案将不包含群聊信息。")
            group_list = []

        # 3. 遍历群列表，获取机器人在每个群的详细信息（群名片、角色等）
        groups_details = {}
        for group in group_list:
            group_id = str(group.get("group_id"))
            if not group_id:
                continue

            # 获取机器人在这个群的成员信息
            member_info = await utils.napcat_get_member_info(
                connection, group_id=group_id, user_id=bot_id
            )
            if member_info:
                groups_details[group_id] = {
                    "group_id": group_id,
                    "group_name": member_info.get("group_name")
                    or group.get("group_name"),  # 优先用成员信息的
                    "card": member_info.get("card", ""),
                    "role": member_info.get("role", "member"),
                    "title": member_info.get("title", ""),
                }
            else:
                groups_details[group_id] = {
                    "group_id": group_id,
                    "group_name": group.get("group_name"),
                    "card": "",
                    "role": "member",
                    "title": "",
                }

        # 4. 组装最终的、丰满的报告
        # 从 event 对象里拿到 platform_id，这是最可靠的来源
        platform_id = event.get_platform()

        full_profile_report = {
            "user_id": bot_id,
            "nickname": bot_nickname,
            "platform": platform_id,
            "groups": groups_details,
        }

        print(f"机器人完整档案已生成，包含 {len(groups_details)} 个群聊信息。")
        return True, "机器人完整档案获取成功。", full_profile_report


# 以后有其他复杂动作，就在这里加新的 Handler 类
# ...

# 最终的复杂动作处理器名录
COMPLEX_ACTION_HANDLERS: dict[str, BaseComplexActionHandler] = {
    "send_forward_message": SendForwardMessageHandler,
    "get_bot_profile": GetBotProfileHandler,
}


# ==============================================================================
# 2. 简单API调用映射 (Simple API Call Mapping)
# ==============================================================================
# 对于那些可以直接映射到 utils.py 中某个 napcat_ 函数的简单动作，
# 我们就用这个字典来登记！
# 格式: "动作别名": (对应的napcat函数, [必需的参数列表])
# ------------------------------------------------------------------------------

# 定义一个类型别名，让代码更清晰，这是天才的优雅！
# (Callable, List of required param names)
ActionMappingType = tuple[Callable[..., Awaitable[dict[str, Any] | None]], list[str]]

# 动作的“圣殿”，记载了所有神权（API）的咒语和贡品（必需参数）
ACTION_MAPPING: dict[str, ActionMappingType] = {
    # --- 好友操作 ---
    "delete_friend": (utils.napcat_delete_friend, ["user_id"]),
    # --- 群组管理 ---
    "kick_member": (utils.napcat_set_group_kick, ["group_id", "user_id"]),
    "ban_member": (utils.napcat_set_group_ban, ["group_id", "user_id"]),
    "ban_all_members": (utils.napcat_set_group_whole_ban, ["group_id", "enable"]),
    "set_member_card": (utils.napcat_set_group_card, ["group_id", "user_id", "card"]),
    "set_member_title": (
        utils.napcat_set_group_special_title,
        ["group_id", "user_id", "special_title"],
    ),
    "leave_conversation": (utils.napcat_set_group_leave, ["group_id"]),
    "set_admin": (utils.napcat_set_group_admin, ["group_id", "user_id", "enable"]),
    "set_conversation_name": (utils.napcat_set_group_name, ["group_id", "group_name"]),
    # --- 消息操作 ---
    "recall_message": (utils.napcat_delete_msg, ["message_id"]),
    "poke_user": (utils.napcat_send_poke, ["target_user_id"]),  # poke现在统一了
    "set_message_emoji_like": (
        utils.napcat_set_msg_emoji_like,
        ["message_id", "emoji_id"],
    ),
    "forward_single_message": (
        utils.napcat_forward_single_msg,
        ["target_id", "target_type", "message_id"],
    ),
    # --- 请求处理 ---
    "handle_friend_request": (utils.napcat_set_friend_add_request, ["flag", "approve"]),
    "handle_group_request": (
        utils.napcat_set_group_add_request,
        ["flag", "sub_type", "approve"],
    ),
    # --- 信息获取 ---
    "get_group_info": (utils.napcat_get_group_info, ["group_id"]),
    "get_member_info": (utils.napcat_get_member_info, ["group_id", "user_id"]),
    "get_stranger_info": (utils.napcat_get_stranger_info, ["user_id"]),
    "get_list": (utils.napcat_get_list, ["list_type"]),
    "get_history": (utils.napcat_get_history, ["conversation_id", "conversation_type"]),
    # --- 文件操作 ---
    "upload_group_file": (utils.napcat_upload_group_file, ["group_id", "file", "name"]),
    "delete_group_file": (
        utils.napcat_delete_group_file,
        ["group_id", "file_id", "busid"],
    ),
    "create_group_folder": (
        utils.napcat_create_group_file_folder,
        ["group_id", "name"],
    ),
    "delete_group_folder": (
        utils.napcat_delete_group_folder,
        ["group_id", "folder_id"],
    ),
    "get_group_files": (
        utils.napcat_get_group_files_by_folder,
        ["group_id"],
    ),  # 简化，默认查根目录或指定目录
    "get_group_file_url": (
        utils.napcat_get_group_file_url,
        ["group_id", "file_id", "busid"],
    ),
    # 将 get_bot_profile 指向新的复合函数
    # 注释：这个动作是为 AIcarusCore 的上线安检流程专门设计的，用于获取机器人的完整档案。
    "get_bot_profile": (utils.napcat_get_bot_profile_for_core, []),
    # --- 其他功能 ---
    "sign_in": (utils.napcat_set_group_sign, ["group_id"]),
    "set_status": (utils.napcat_set_online_status, ["status"]),
    "set_avatar": (utils.napcat_set_qq_avatar, ["file"]),
    "get_group_honor_info": (utils.napcat_get_group_honor_info, ["group_id", "type"]),
    "send_group_notice": (utils.napcat_send_group_notice, ["group_id", "content"]),
    "get_group_notice": (utils.napcat_get_group_notice, ["group_id"]),
    "get_recent_contacts": (utils.napcat_get_recent_contact, []),
    "get_ai_characters": (utils.napcat_get_ai_characters, ["group_id"]),
    "send_ai_voice": (
        utils.napcat_send_group_ai_record,
        ["group_id", "character", "text"],
    ),
}
