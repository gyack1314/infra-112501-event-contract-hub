"""任务脚本常见命名的兼容入口。"""

from contract_hub.cli import main
from event_contract_middleware import *  # noqa: F401,F403

if __name__ == "__main__":
    raise SystemExit(main())
