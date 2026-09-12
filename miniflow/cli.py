from __future__ import annotations

import argparse
import os


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="miniflow", description="MiniFlow task queue")
    subparsers = parser.add_subparsers(dest="command", required=True)

    serve = subparsers.add_parser("serve", help="Run the API, dashboard and workers")
    serve.add_argument("--host", default="127.0.0.1")
    serve.add_argument("--port", type=int, default=8000)
    serve.add_argument("--workers", type=int, default=2, help="Worker thread count")
    serve.add_argument("--database", default="miniflow.db")
    serve.add_argument("--reload", action="store_true", help="Reload when source files change")
    return parser


def main() -> None:
    args = build_parser().parse_args()
    if args.command == "serve":
        os.environ["MINIFLOW_DB"] = args.database
        os.environ["MINIFLOW_WORKERS"] = str(args.workers)
        import uvicorn

        uvicorn.run("miniflow.api:app", host=args.host, port=args.port, reload=args.reload)


if __name__ == "__main__":
    main()
