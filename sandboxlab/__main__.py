"""Entry point: ``python3 -m sandboxlab [--data DIR] [--port N]``."""

from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path

from .app import make_server
from .store import Store


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        prog="sandboxlab",
        description="多沙箱克隆 / 刷新 / 销毁与源记录删除的实验环境",
    )
    parser.add_argument(
        "--data",
        default=os.environ.get("SANDBOXLAB_DATA", str(Path.cwd() / "var")),
        help="数据目录（默认 ./var，可用 SANDBOXLAB_DATA 覆盖）",
    )
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=8080)
    parser.add_argument(
        "--recover",
        action="store_true",
        help="启动前完成任何中断的删除清理，然后退出（不启动服务）",
    )
    args = parser.parse_args(argv)
    if args.recover:
        store = Store(args.data)
        store.ensure_seeded()
        result = store.recover()
        print(f"恢复完成：处理 {result['recovered']} 条未完成的删除")
        return 0
    server = make_server(args.data, args.host, args.port)
    host, port = server.server_address[:2]
    print(f"SandboxLab 已启动 → http://{host}:{port}/  (数据目录: {Path(args.data).resolve()})")
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("\n已停止。")
    finally:
        server.server_close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
