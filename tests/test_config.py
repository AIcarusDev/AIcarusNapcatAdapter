from collections.abc import Generator
from pathlib import Path

# 模拟 sys.exit() 行为，以便我们可以捕获它
from unittest.mock import MagicMock, patch

import pytest
import tomlkit

# 导入需要测试的模块
from src import config
from src.logger import logger

# 禁用日志输出，保持测试清洁
logger.remove()

# 定义不同版本的模板内容
TEMPLATE_V1 = """
config_version = "1.0.0"

[adapter_server]
host = "127.0.0.1"
port = 8095

[core_connection]
url = "ws://localhost:8077"
platform_id = "napcat_test"

[bot_settings]
force_self_id = ""
"""

TEMPLATE_V2 = """
config_version = "2.0.0" # 版本升级

# [adapter_server] 保持不变
[adapter_server]
host = "127.0.0.1"
port = 8095

[core_connection]
url = "ws://localhost:8077"
platform_id = "napcat_test"

[bot_settings]
force_self_id = ""
napcat_heartbeat_interval_seconds = 30 # 新增配置项

# 新增一个配置段
[log_settings]
level = "INFO"
"""


@pytest.fixture
def temp_config_env(tmp_path: Path) -> Generator[tuple[Path, MagicMock], None, None]:
    """一个 pytest fixture，用于创建一个临时的、隔离的配置环境.

    它会模拟项目根目录，并在测试结束后自动清理.
    """
    config._global_config_instance = None

    project_root = tmp_path
    # 模拟模块中的全局变量，让它们指向我们的临时目录
    config.PROJECT_ROOT = project_root
    config.TEMPLATE_CONFIG_PATH = project_root / "template" / "config_template.toml"
    config.ACTUAL_CONFIG_PATH = project_root / "config.toml"
    config.BACKUP_DIR = project_root / "config_backups"

    # 创建必要的目录
    config.TEMPLATE_CONFIG_PATH.parent.mkdir()
    config.BACKUP_DIR.mkdir()

    with patch("sys.exit") as mock_exit:
        yield project_root, mock_exit

    # 清理工作可以保持不变，或者直接移除，因为 fixture 开始时已经重置了
    config._global_config_instance = None


def test_config_creation_on_first_run(temp_config_env: None) -> None:
    """测试：当配置文件不存在时，应能从模板正确创建."""
    project_root, mock_exit = temp_config_env

    # 准备：只创建模板文件
    config.TEMPLATE_CONFIG_PATH.write_text(TEMPLATE_V1, encoding="utf-8")

    # 执行：调用配置加载函数
    config.load_and_get_config()

    # 断言：
    # 1. sys.exit(0) 应该被调用，提示用户检查配置
    mock_exit.assert_called_once_with(0)
    # 2. 实际的配置文件应该被创建，且内容与模板一致
    assert config.ACTUAL_CONFIG_PATH.exists()
    assert config.ACTUAL_CONFIG_PATH.read_text(encoding="utf-8") == TEMPLATE_V1


def test_config_version_update_and_merge(temp_config_env: None) -> None:
    """测试：当配置文件版本过旧时，应能备份并合并用户设置到新版模板."""
    project_root, mock_exit = temp_config_env

    # 准备：
    # 1. 创建一个V2版本的模板
    config.TEMPLATE_CONFIG_PATH.write_text(TEMPLATE_V2, encoding="utf-8")

    # 2. 创建一个V1版本的、被用户修改过的实际配置文件
    user_modified_v1_config = tomlkit.parse(TEMPLATE_V1)
    user_modified_v1_config["core_connection"]["platform_id"] = "my_awesome_bot"
    user_modified_v1_config["adapter_server"]["port"] = 9999
    config.ACTUAL_CONFIG_PATH.write_text(tomlkit.dumps(user_modified_v1_config), encoding="utf-8")

    # 执行：
    config.load_and_get_config()

    # 断言：
    # 1. sys.exit(0) 应被调用
    mock_exit.assert_called_once_with(0)
    # 2. 备份目录应该有且仅有一个备份文件
    assert len(list(config.BACKUP_DIR.iterdir())) == 1
    # 3. 新的配置文件内容检查
    new_config_doc = tomlkit.parse(config.ACTUAL_CONFIG_PATH.read_text(encoding="utf-8"))
    assert new_config_doc["config_version"] == "2.0.0"  # 版本号已更新
    assert new_config_doc["adapter_server"]["port"] == 9999  # 用户修改的值被保留
    assert (
        new_config_doc["core_connection"]["platform_id"] == "my_awesome_bot"
    )  # 用户修改的值被保留
    assert new_config_doc["log_settings"]["level"] == "INFO"  # 新模板的配置项存在
