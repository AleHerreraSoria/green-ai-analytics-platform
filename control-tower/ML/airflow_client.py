"""
Cliente REST mínimo para Airflow API v1 (misma convención de env que control-tower).
Carga `control-tower/.env` vía utils.project_env (alineado con el resto de la app).
"""

from __future__ import annotations

import base64
import json
import os
import re
import sys
import threading
from dataclasses import dataclass
from pathlib import Path
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.parse import quote, urlencode, urlparse
from urllib.request import HTTPCookieProcessor, Request, build_opener, urlopen

_CT_ROOT = Path(__file__).resolve().parents[1]
if str(_CT_ROOT) not in sys.path:
    sys.path.insert(0, str(_CT_ROOT))

from utils.project_env import ensure_dotenv_loaded

ensure_dotenv_loaded()


@dataclass(slots=True)
class AirflowAPIConfig:
    base_url: str
    dag_id: str
    username: str | None
    password: str | None
    token: str | None
    authorization_header: str | None
    timeout_seconds: int = 30


def _resolve_dag_id() -> str:
    raw = os.getenv("PIPELINE_DAG_ID", "").strip()
    return raw or "green-ai-full-pipeline"


def _connection_runtime_error(base_url: str, exc: BaseException | str, where: str) -> RuntimeError:
    """Convierte fallos de red en un mensaje accionable (sobre todo desde Docker)."""
    msg = str(exc)
    low = msg.lower()
    timeout_like = (
        isinstance(exc, TimeoutError)
        or "timed out" in low
        or "timeout" in low
    )
    lines = [
        f"No se pudo conectar con Airflow ({where}).",
        f"URL configurada: {base_url}",
        f"Error: {msg}",
        "",
        "Desde el contenedor, esa URL debe ser alcanzable (no es la misma red que tu navegador). "
        "Si Airflow no está en el mismo docker-compose, evitá localhost apuntando al host: "
        "usá IP/DNS interno de la VPC, el hostname del servicio, o host.docker.internal donde exista.",
        "Revisá security groups, firewall y puerto.",
    ]
    if timeout_like:
        lines.append("Si la red es lenta, probá subir AIRFLOW_API_TIMEOUT_SECONDS (p. ej. 120).")
    if "login" in where.lower():
        lines.append(
            "Si el fallo es al abrir /login/ pero la API REST admite usuario/clave sin cookie web, "
            "poné AIRFLOW_API_DISABLE_FAB_SESSION=1 en `.env` y reiniciá el contenedor (solo Basic contra la API)."
        )
    lines.append(
        "Si tu API acepta token, podés usar AIRFLOW_API_TOKEN o AIRFLOW_API_AUTHORIZATION "
        "y AIRFLOW_API_DISABLE_FAB_SESSION=1 para no pasar por el login web /login/."
    )
    return RuntimeError("\n".join(lines))


def config_from_env() -> AirflowAPIConfig:
    auth_raw = os.getenv("AIRFLOW_API_AUTHORIZATION", "").strip() or None
    return AirflowAPIConfig(
        base_url=os.getenv("AIRFLOW_API_BASE_URL", "").strip().rstrip("/"),
        dag_id=_resolve_dag_id(),
        username=os.getenv("AIRFLOW_API_USERNAME"),
        password=os.getenv("AIRFLOW_API_PASSWORD"),
        token=os.getenv("AIRFLOW_API_TOKEN"),
        authorization_header=auth_raw,
        timeout_seconds=int(os.getenv("AIRFLOW_API_TIMEOUT_SECONDS", "30")),
    )


class AirflowRESTClient:
    def __init__(self, config: AirflowAPIConfig):
        self.config = config
        self.base_url = config.base_url.rstrip("/")
        self._fab_lock = threading.Lock()
        self._fab_cookie_header: str | None = None
        self._fab_login_failed = False

    def _should_use_fab_session(self) -> bool:
        if os.getenv("AIRFLOW_API_DISABLE_FAB_SESSION", "").strip().lower() in {"1", "true", "yes"}:
            return False
        if self.config.authorization_header or self.config.token:
            return False
        return bool(self.config.username and self.config.password)

    def _perform_fab_login(self) -> str:
        from http.cookiejar import CookieJar

        login_url = f"{self.base_url}/login/"
        jar = CookieJar()
        opener = build_opener(HTTPCookieProcessor(jar))
        try:
            with opener.open(
                Request(login_url, headers={"Accept": "text/html"}),
                timeout=self.config.timeout_seconds,
            ) as get_resp:
                html = get_resp.read().decode("utf-8", errors="replace")
            m = re.search(r'id="csrf_token"[^>]+value="([^"]+)"', html) or re.search(
                r'name="csrf_token"[^>]+value="([^"]+)"', html
            )
            if not m:
                raise RuntimeError("No se encontró csrf_token en /login/ de Airflow.")
            csrf = m.group(1)
            body = urlencode(
                {
                    "csrf_token": csrf,
                    "username": self.config.username or "",
                    "password": self.config.password or "",
                }
            ).encode("utf-8")
            post_req = Request(
                login_url,
                data=body,
                method="POST",
                headers={
                    "Content-Type": "application/x-www-form-urlencoded",
                    "Accept": "text/html",
                    "Referer": login_url,
                },
            )
            with opener.open(post_req, timeout=self.config.timeout_seconds) as post_resp:
                post_resp.read()
                final_url = post_resp.geturl()
        except (URLError, TimeoutError, OSError) as exc:
            raise _connection_runtime_error(self.base_url, exc, "login web FAB (/login/)") from exc
        if "/login" in urlparse(final_url).path.lower():
            raise RuntimeError("Login FAB falló: usuario o contraseña incorrectos.")
        parts = [f"{c.name}={c.value}" for c in jar]
        if not parts:
            raise RuntimeError("Tras el login FAB no hubo cookies.")
        return "; ".join(parts)

    def _ensure_fab_session_cookie(self) -> None:
        if not self._should_use_fab_session():
            return
        if self._fab_cookie_header is not None or self._fab_login_failed:
            return
        with self._fab_lock:
            if self._fab_cookie_header is not None or self._fab_login_failed:
                return
            try:
                self._fab_cookie_header = self._perform_fab_login()
            except Exception:
                self._fab_login_failed = True
                raise

    def _auth_header(self) -> dict[str, str]:
        if self.config.authorization_header:
            value = self.config.authorization_header.strip()
            lower = value.lower()
            if lower.startswith("bearer ") or lower.startswith("basic "):
                return {"Authorization": value}
            return {"Authorization": f"Bearer {value}"}
        if self.config.token:
            return {"Authorization": f"Bearer {self.config.token}"}
        if self.config.username and self.config.password:
            raw = f"{self.config.username}:{self.config.password}".encode("utf-8")
            encoded = base64.b64encode(raw).decode("ascii")
            return {"Authorization": f"Basic {encoded}"}
        return {}

    def get_json(self, endpoint: str) -> dict[str, Any]:
        if not self.base_url:
            raise ValueError("AIRFLOW_API_BASE_URL no configurado.")
        self._ensure_fab_session_cookie()
        url = f"{self.base_url}{endpoint}"
        headers = {"Accept": "application/json"} | self._auth_header()
        if self._fab_cookie_header:
            headers["Cookie"] = self._fab_cookie_header
        request = Request(url=url, headers=headers, method="GET")
        try:
            with urlopen(request, timeout=self.config.timeout_seconds) as response:
                return json.loads(response.read().decode("utf-8"))
        except HTTPError as exc:
            detail = ""
            try:
                raw = exc.read().decode("utf-8", errors="replace").strip()
                if raw:
                    detail = f" Detalle: {raw[:800]}"
            except OSError:
                pass
            hint = ""
            if exc.code == 401:
                hint = (
                    "\n\n401 Unauthorized: usuario/clave incorrectos o modo de auth no aceptado por esta "
                    "instancia. Verificá AIRFLOW_API_USERNAME y AIRFLOW_API_PASSWORD en `.env`, o definí "
                    "AIRFLOW_API_TOKEN / AIRFLOW_API_AUTHORIZATION (JWT Bearer). "
                    "El usuario debe existir en Airflow con permisos RBAC sobre el DAG/API. "
                    "Si tenés AIRFLOW_API_DISABLE_FAB_SESSION=1, solo se usa Basic; si Airflow exige JWT o "
                    "sesión web, quitá esa variable para intentar login FAB (cookie) o pasá un token."
                )
            raise RuntimeError(f"Airflow API {exc.code} para {endpoint}.{detail}{hint}") from exc
        except URLError as exc:
            inner: BaseException | str = exc.reason if exc.reason is not None else exc
            raise _connection_runtime_error(self.base_url, inner, f"API GET {endpoint}") from exc
