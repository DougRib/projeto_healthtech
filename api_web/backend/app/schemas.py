"""Modelos de resposta da API."""

from typing import List, Optional
from pydantic import BaseModel


class OperadoraResumo(BaseModel):
    cnpj: str
    razao_social: str
    registro_ans: Optional[str] = None
    modalidade: Optional[str] = None
    uf: Optional[str] = None
    tem_despesas: Optional[bool] = None


class OperadorasPaginadas(BaseModel):
    data: List[OperadoraResumo]
    total: int
    page: int
    limit: int


class DespesaTrimestre(BaseModel):
    ano: int
    trimestre: int
    valor_total: float


class EstatisticasResposta(BaseModel):
    total_despesas: float
    media_despesas: float
    media_por_operadora: float
    total_operadoras: int
    top_1_operadora: Optional[dict] = None
    top_5_operadoras: List[dict]
    despesas_por_uf: List[dict]
    uf_lider: Optional[dict] = None
    operadoras_alta_variabilidade: int
