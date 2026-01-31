"""API FastAPI - Etapa 4."""

from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware

from .config import Settings
from .data_loader import DataRepository, limpar_cnpj
from .schemas import OperadorasPaginadas, OperadoraResumo, DespesaTrimestre, EstatisticasResposta


settings = Settings()
repo = DataRepository(settings)

app = FastAPI(title=settings.api_title, version=settings.api_version)

app.add_middleware(
    CORSMiddleware,
    allow_origins=[origem.strip() for origem in settings.cors_origins.split(",") if origem.strip()],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.get("/api/operadoras", response_model=OperadorasPaginadas)
def listar_operadoras(
    page: int = Query(1, ge=1),
    limit: int = Query(20, ge=1, le=200),
    search: str | None = None,
):
    return repo.listar_operadoras(page=page, limit=limit, search=search)


@app.get("/api/operadoras/{cnpj}", response_model=OperadoraResumo)
def obter_operadora(cnpj: str):
    cnpj = limpar_cnpj(cnpj)
    operadora = repo.obter_operadora(cnpj)
    if not operadora:
        raise HTTPException(status_code=404, detail="Operadora nao encontrada")
    return operadora


@app.get("/api/operadoras/{cnpj}/despesas", response_model=list[DespesaTrimestre])
def obter_despesas(cnpj: str):
    cnpj = limpar_cnpj(cnpj)
    historico = repo.obter_historico(cnpj)
    return historico


@app.get("/api/estatisticas", response_model=EstatisticasResposta)
def obter_estatisticas():
    return repo.obter_estatisticas()
