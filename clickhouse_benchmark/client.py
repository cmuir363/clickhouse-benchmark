import dataclasses
import json
import logging
import socket
import subprocess
from functools import cached_property
from pathlib import Path
from typing import Any
from uuid import uuid4

LOG = logging.getLogger(__name__)


@dataclasses.dataclass
class Output:
    query_id: str
    output: bytes


@dataclasses.dataclass
class Result:
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
        res = self._execute(query, settings, input, host, format="JSON")
        return Result(query_id=res.query_id, results=json.loads(res.output)["data"])

    def execute_no_result(
        self,
        query: str,
        settings: dict[str, Any] | None = None,
        input: Path | None = None,
        host: str | None = None,
    ) -> str:
        res = self._execute(query, settings, input, host, format="Null")
        return res.query_id

    def execute_all_nodes(
        self,
        query: str,
        settings: dict[str, Any] | None = None,
        input: Path | None = None,
    ) -> list[Result]:
        res = []
        import concurrent.futures

        with concurrent.futures.ThreadPoolExecutor() as executor:
            futures = [
                executor.submit(self.execute, query, settings, input, host)
                for host in self._hosts
            ]
            for future in concurrent.futures.as_completed(futures):
                if exception := future.exception():
                    raise exception
                res.append(future.result())
        return res

    @cached_property
    def _hosts(self) -> list[str]:
        return list(set([i[4][0] for i in socket.getaddrinfo(self.host, None)]))

    def _execute(
        self,
        query: str,
        settings: dict[str, Any] | None = None,
        input: Path | None = None,
        host: str | None = None,
        format: str = "JSON",
    ) -> Output:
        LOG.info("Running query: %s", query)
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
            f"--format={format}",
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
        return Output(query_id=query_id, output=stdout)
