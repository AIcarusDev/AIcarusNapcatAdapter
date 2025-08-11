import pytest
from aicarus_protocols import Event, Seg
from pytest_mock import MockerFixture
from src.event_definitions import MessageEventFactory
from src.logger import logger
from src.recv_handler_aicarus import RecvHandlerAicarus

# 禁用日志输出
logger.remove()

# --- 模拟数据 ---

# 一个典型的Napcat群消息事件
MOCK_NAPCAT_GROUP_MESSAGE = {
    "post_type": "message",
    "message_type": "group",
    "sub_type": "normal",
    "message_id": "-123456789",
    "group_id": 10001,
    "user_id": 20002,
    "self_id": 30003,
    "time": 1678886400,
    "sender": {"user_id": 20002, "nickname": "测试用户", "card": "群名片", "role": "member"},
    "message": [
        {"type": "text", "data": {"text": "你好 "}},
        {"type": "at", "data": {"qq": "30003"}},
        {
            "type": "image",
            "data": {"file": "some_file_id.jpg", "url": "http://example.com/img.jpg"},
        },
    ],
}


@pytest.fixture
def recv_handler(mocker: MockerFixture) -> RecvHandlerAicarus:
    """Fixture: 创建一个 RecvHandlerAicarus 实例，并模拟其依赖."""
    handler = RecvHandlerAicarus()

    # [FIX] 模拟一个存在的服务器连接，使其不为 None
    handler.server_connection = mocker.Mock()

    # 模拟 config，防止它真的去读文件
    mocker.patch.object(handler, "global_config")
    handler.global_config.core_platform_id = "test_platform"

    # 模拟异步工具函数，让它们立即返回预设值，避免网络调用
    mocker.patch(
        "src.recv_handler_aicarus.napcat_get_group_info", return_value={"group_name": "测试群"}
    )
    # 为了简化，这里让获取成员信息直接返回空，测试会使用sender里的数据
    mocker.patch("src.recv_handler_aicarus.napcat_get_member_info", return_value={})
    mocker.patch(
        "src.recv_handler_aicarus.process_image_url_to_aicarus_seg",
        return_value=Seg(type="image", data={"summary": "mocked_image"}),
    )

    return handler


@pytest.mark.asyncio
async def test_group_message_conversion(recv_handler: RecvHandlerAicarus) -> None:
    """测试：验证一个标准的Napcat群消息能否被正确转换为AIcarus事件."""
    factory = MessageEventFactory()

    # 执行转换
    aicarus_event = await factory.create_event(MOCK_NAPCAT_GROUP_MESSAGE, recv_handler)

    # --- 断言验证 ---
    assert isinstance(aicarus_event, Event)

    # 1. 验证事件元数据
    assert aicarus_event.event_type == "message.test_platform.group.normal"
    assert aicarus_event.bot_id == "30003"

    # 2. 验证会话信息
    assert aicarus_event.conversation_info is not None
    assert aicarus_event.conversation_info.type == "group"
    assert aicarus_event.conversation_info.conversation_id == "10001"
    assert aicarus_event.conversation_info.name == "测试群"

    # 3. 验证用户信息
    assert aicarus_event.user_info is not None
    assert aicarus_event.user_info.user_id == "20002"
    assert aicarus_event.user_info.user_nickname == "测试用户"
    assert aicarus_event.user_info.user_cardname == "群名片"

    # 4. 验证消息内容 (Content Segments)
    # content[0] 是 message_metadata, content[1] 是 text, content[2] 是 at, ...
    assert len(aicarus_event.content) == 4

    text_seg = aicarus_event.content[1]
    assert text_seg.type == "text"
    assert text_seg.data["text"] == "你好 "

    at_seg = aicarus_event.content[2]
    assert at_seg.type == "at"
    assert at_seg.data["user_id"] == "30003"

    image_seg = aicarus_event.content[3]
    assert image_seg.type == "image"
    assert image_seg.data["summary"] == "mocked_image"
