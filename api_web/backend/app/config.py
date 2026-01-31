"""Configuracoes do backend."""

from pathlib import Path
from pydantic_settings import BaseSettings


ROOT_DIR = Path(__file__).resolve().parents[3]

DEFAULT_CONSOLIDADO = ROOT_DIR / "output" / "consolidado_despesas.csv"
FALLBACK_CONSOLIDADO = ROOT_DIR / "integracao_api" / "consolidado_despesas.csv"
DEFAULT_AGREGADO = ROOT_DIR / "output" / "despesas_agregadas.csv"
FALLBACK_AGREGADO = ROOT_DIR / "transformacao" / "despesas_agregadas.csv"
DEFAULT_CADASTRO = ROOT_DIR / "data" / "cadastro_operadoras.csv"


class Settings(BaseSettings):
    """Variaveis de ambiente do projeto."""

    api_title: str = "Healthtech API"
    api_version: str = "1.0.0"
    api_cache_ttl_seconds: int = 300
    cors_origins: str = "http://localhost:5173"

    data_consolidado_path: str = ""
    data_agregado_path: str = ""
    data_cadastro_path: str = ""

    class Config:
        env_file = str(ROOT_DIR / ".env")
        env_prefix = "HT_"


def resolve_path(primary: str, *fallbacks: Path) -> Path | None:
    if primary:
        path = Path(primary)
        if path.exists():
            return path
    for fallback in fallbacks:
        if fallback.exists():
            return fallback
    return None
