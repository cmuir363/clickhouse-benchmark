import dataclasses
import json
import logging
import socket
import subprocess
from pathlib import Path
from typing import Any
from uuid import uuid4

LOG = logging.getLogger(__name__)


@dataclasses.dataclass
class Result:
    query_id: str
    output: bytes


@dataclasses.dataclass
class JsonResult:
    query_id: str
    results: list[dict[str, Any]]


@dataclasses.dataclass
class ClickHouseClient:
    host: str
    port: int
    user: str
    password: str
    database: str
    secure: bool

    def execute(
        self,
        query: str,
        settings: dict[str, Any] | None = None,
        input: Path | None = None,
        host: str | None = None,
    ) -> Result:
        LOG.info("Running query: %s", query[:500] + "...")
        if not host:
            host = self.host
        query_id = str(uuid4())
        cmd = [
            "clickhouse",
            "client",
            "--multiquery",
            "--multiline",
            "--host",
            host,
            "--port",
            str(self.port),
            "--database",
            self.database,
            "--user",
            self.user,
            "--password",
            self.password,
            "--query",
            query,
            "--query_id",
            query_id,
        ]
        if self.secure:
            cmd.append("--secure")
        if settings is not None:
            for key, val in settings.items():
                cmd.extend(["--" + key, str(val)])
        LOG.debug("Running clickhouse-client: %s", cmd)
        if input:
            with input.open("rb") as f:
                process = subprocess.Popen(
                    cmd, stdin=f, stdout=subprocess.PIPE, stderr=subprocess.PIPE
                )
        else:
            process = subprocess.Popen(
                cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE
            )
        stdout, stderr = process.communicate()
        if process.returncode:
            raise RuntimeError(
                f"clickhouse client failed with {process.returncode}: {stderr.decode()}"
            )
        return Result(query_id=query_id, output=stdout)

    def execute_json(
        self,
        query: str,
        settings: dict[str, Any] | None = None,
        input: Path | None = None,
        host: str | None = None,
    ) -> JsonResult:
        query = query.rstrip("FORMAT JSON") + " FORMAT JSON"
        res = self.execute(query, settings, input, host)
        return JsonResult(query_id=res.query_id, results=json.loads(res.output)["data"])

    def execute_all_nodes(
        self,
        query: str,
        settings: dict[str, Any] | None = None,
        input: Path | None = None,
    ) -> list[Result]:
        res = []
        for host in self.hosts():
            res.append(self.execute(query, settings, input, host))
        return res

    def hosts(self) -> list[str]:
        return list(set([i[4][0] for i in socket.getaddrinfo(self.host, None)]))
