from pathlib import Path

import pytest
from aioresponses import aioresponses
from src.logger import logger

# 确保测试脚本可以找到 src 目录下的模块
# 如果你的项目结构标准，pytest 会自动处理
from src.utils import process_image_url_to_aicarus_seg

# 禁用 loguru 在测试期间的输出，保持结果清爽
logger.remove()

# --- 测试设置 ---

# 定义测试用的假URL
STATIC_IMAGE_URL = "http://fake-server.com/static.jpg"
ANIMATED_IMAGE_URL = "http://fake-server.com/animated.gif"
NOT_FOUND_URL = "http://fake-server.com/not-found.png"
INVALID_DATA_URL = "http://fake-server.com/not-an-image.txt"

# 获取测试素材的路径
FIXTURES_DIR = Path(__file__).parent / "fixtures"
STATIC_IMAGE_PATH = FIXTURES_DIR / "static.jpg"
ANIMATED_IMAGE_PATH = FIXTURES_DIR / "animated.gif"

# --- 测试用例 ---


@pytest.mark.asyncio
async def test_process_static_image_success() -> None:
    """测试：当URL指向一个标准的静态图片时.

    应能正确处理并返回一个 'image' 类型的 Seg.
    """
    # 读取本地静态图片文件作为模拟响应体
    with open(STATIC_IMAGE_PATH, "rb") as f:
        image_data = f.read()

    # 使用 aioresponses 模拟网络请求
    with aioresponses() as m:
        # 当代码请求 STATIC_IMAGE_URL 时，返回我们准备好的图片数据
        m.get(STATIC_IMAGE_URL, status=200, body=image_data)

        # 调用被测试的函数
        result_seg = await process_image_url_to_aicarus_seg(STATIC_IMAGE_URL, "file123")

        # --- 断言验证 ---
        assert result_seg is not None
        assert result_seg.type == "image", "对于静态图，Seg类型应为 'image'"
        assert result_seg.data["summary"] == "image"
        assert result_seg.data["file_id"] == "file123"
        assert "base64" in result_seg.data
        assert len(result_seg.data["base64"]) > 100, "Base64编码不应为空"


@pytest.mark.asyncio
async def test_process_animated_image_success() -> None:
    """测试：当URL指向一个标准的动图(GIF)时.

    应能正确处理并返回一个 'video' 类型的 Seg.
    """
    with open(ANIMATED_IMAGE_PATH, "rb") as f:
        gif_data = f.read()

    with aioresponses() as m:
        m.get(ANIMATED_IMAGE_URL, status=200, body=gif_data)

        result_seg = await process_image_url_to_aicarus_seg(ANIMATED_IMAGE_URL, "file456")

        # --- 断言验证 ---
        assert result_seg is not None
        assert result_seg.type == "video", "对于动图，Seg类型应为 'video'"
        assert result_seg.data["summary"] == "animated_sticker"
        assert result_seg.data["file_id"] == "file456"
        assert result_seg.data["mime_type"] == "video/mp4"
        assert "base64" in result_seg.data
        assert len(result_seg.data["base64"]) > 100, "转换后的视频Base64不应为空"


@pytest.mark.asyncio
async def test_process_image_download_failure() -> None:
    """测试：当URL返回404 Not Found.

    应能优雅地失败并返回一个提示下载失败的 'text' Seg.
    """
    with aioresponses() as m:
        # 模拟一个404错误
        m.get(NOT_FOUND_URL, status=404)

        result_seg = await process_image_url_to_aicarus_seg(NOT_FOUND_URL)

        # --- 断言验证 ---
        assert result_seg is not None
        assert result_seg.type == "text"
        assert "下载失败" in result_seg.data["text"]


@pytest.mark.asyncio
async def test_process_invalid_image_data() -> None:
    """测试：当URL返回的不是图片数据时（例如一个文本文件）.

    应能优雅地失败并返回一个提示处理异常的 'text' Seg.
    """
    invalid_data = b"This is just a plain text, not an image."
    with aioresponses() as m:
        m.get(INVALID_DATA_URL, status=200, body=invalid_data)

        result_seg = await process_image_url_to_aicarus_seg(INVALID_DATA_URL)

        # --- 断言验证 ---
        assert result_seg is not None
        assert result_seg.type == "text"
        assert "处理异常" in result_seg.data["text"]
