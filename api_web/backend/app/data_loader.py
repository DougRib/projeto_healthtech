"""Carregamento e acesso aos dados."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Optional

import pandas as pd

from .config import Settings, resolve_path, DEFAULT_CONSOLIDADO, DEFAULT_AGREGADO, DEFAULT_CADASTRO, FALLBACK_CONSOLIDADO, FALLBACK_AGREGADO


def limpar_cnpj(valor: str) -> str:
    digitos = "".join(ch for ch in str(valor) if ch.isdigit())
    return digitos.zfill(14) if digitos else ""


def _normalizar_texto(valor: str) -> str:
    if valor is None:
        return ""
    texto = str(valor).strip().upper()
    while "  " in texto:
        texto = texto.replace("  ", " ")
    return texto


def _mapear_colunas(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df.columns = df.columns.str.strip().str.lower()
    mapeamento = {
        "cnpj": "cnpj",
        "razaosocial": "razao_social",
        "razao_social": "razao_social",
        "registroans": "registro_ans",
        "registro_ans": "registro_ans",
        "registro_operadora": "registro_ans",
        "registro_operadora_ans": "registro_ans",
        "modalidade": "modalidade",
        "uf": "uf",
        "trimestre": "trimestre",
        "ano": "ano",
        "valordespesas": "valor_despesas",
        "valor_despesas": "valor_despesas",
        "totaldespesas": "total_despesas",
        "total_despesas": "total_despesas",
        "mediadespesas": "media_despesas",
        "media_despesas": "media_despesas",
        "mediaportrimestre": "media_por_trimestre",
        "media_por_trimestre": "media_por_trimestre",
        "desviopadrao": "desvio_padrao",
        "desvio_padrao": "desvio_padrao",
        "coeficientevariacao": "coeficiente_variacao",
        "coeficiente_variacao": "coeficiente_variacao",
        "numerotrimestres": "numero_trimestres",
        "numero_trimestres": "numero_trimestres",
        "altavariabilidade": "alta_variabilidade",
        "alta_variabilidade": "alta_variabilidade",
        "ranking": "ranking",
    }
    df = df.rename(columns=mapeamento)
    return df


@dataclass
class CacheItem:
    valor: dict
    expira_em: datetime


class DataRepository:
    """Repositorio em memoria para os dados da API."""

    def __init__(self, settings: Settings) -> None:
        self.settings = settings
        self.consolidado_path = resolve_path(
            settings.data_consolidado_path,
            DEFAULT_CONSOLIDADO,
            FALLBACK_CONSOLIDADO,
        )
        self.agregado_path = resolve_path(
            settings.data_agregado_path,
            DEFAULT_AGREGADO,
            FALLBACK_AGREGADO,
        )
        self.cadastro_path = resolve_path(
            settings.data_cadastro_path,
            DEFAULT_CADASTRO,
        )

        self.df_despesas = pd.DataFrame()
        self.df_agregado = pd.DataFrame()
        self.df_cadastro = pd.DataFrame()
        self.df_operadoras = pd.DataFrame()
        self.cnpjs_com_despesas: set[str] = set()
        self.df_despesas_cnpj = pd.DataFrame()
        self.razoes_com_despesas: set[str] = set()
        self.totais_por_cnpj: dict[str, float] = {}
        self.totais_por_razao: dict[str, float] = {}
        self._cache_stats: Optional[CacheItem] = None

        self._carregar_dados()

    def _carregar_csv(self, caminho: Path) -> pd.DataFrame:
        return pd.read_csv(caminho, sep=";", encoding="utf-8", low_memory=False)

    def _carregar_dados(self) -> None:
        if self.consolidado_path and self.consolidado_path.exists():
            self.df_despesas = _mapear_colunas(self._carregar_csv(self.consolidado_path))
        if self.agregado_path and self.agregado_path.exists():
            self.df_agregado = _mapear_colunas(self._carregar_csv(self.agregado_path))
        if self.cadastro_path and self.cadastro_path.exists():
            self.df_cadastro = _mapear_colunas(self._carregar_csv(self.cadastro_path))

        self._normalizar()

    def _normalizar(self) -> None:
        if not self.df_despesas.empty:
            self.df_despesas["cnpj"] = self.df_despesas["cnpj"].apply(limpar_cnpj)
            self.df_despesas["valor_despesas"] = pd.to_numeric(
                self.df_despesas.get("valor_despesas"), errors="coerce"
            )
            self.df_despesas["ano"] = pd.to_numeric(self.df_despesas.get("ano"), errors="coerce")
            self.df_despesas["trimestre"] = pd.to_numeric(
                self.df_despesas.get("trimestre"), errors="coerce"
            )
            self.cnpjs_com_despesas = set(
                self.df_despesas["cnpj"].dropna().astype(str)
            )
            self.df_despesas_cnpj = (
                self.df_despesas[["cnpj"]]
                .dropna()
                .assign(cnpj=lambda df: df["cnpj"].astype(str).apply(limpar_cnpj))
                .loc[lambda df: df["cnpj"] != ""]
                .drop_duplicates()
                .assign(tem_despesas=True)
            )
            if "razao_social" in self.df_despesas.columns:
                self.razoes_com_despesas = set(
                    self.df_despesas["razao_social"]
                    .fillna("")
                    .apply(_normalizar_texto)
                    .loc[lambda s: s != ""]
                )
                self.totais_por_razao = (
                    self.df_despesas.assign(
                        razao_norm=self.df_despesas["razao_social"]
                        .fillna("")
                        .apply(_normalizar_texto)
                    )
                    .groupby("razao_norm")["valor_despesas"]
                    .sum()
                    .to_dict()
                )
            self.totais_por_cnpj = (
                self.df_despesas.groupby("cnpj")["valor_despesas"].sum().to_dict()
                if "cnpj" in self.df_despesas.columns
                else {}
            )

        if not self.df_agregado.empty:
            self.df_agregado["total_despesas"] = pd.to_numeric(
                self.df_agregado.get("total_despesas"), errors="coerce"
            )

        if not self.df_cadastro.empty:
            self.df_cadastro["cnpj"] = self.df_cadastro["cnpj"].apply(limpar_cnpj)
            self.df_cadastro["razao_social"] = self.df_cadastro.get("razao_social")
            if "uf" in self.df_cadastro.columns:
                self.df_cadastro["uf"] = (
                    self.df_cadastro["uf"]
                    .fillna("")
                    .astype(str)
                    .str.strip()
                    .replace({"nan": ""})
                )
            if "registro_ans" in self.df_cadastro.columns:
                self.df_cadastro["registro_ans"] = (
                    self.df_cadastro["registro_ans"]
                    .fillna("")
                    .astype(str)
                    .str.strip()
                    .replace({"nan": ""})
                )

        self._montar_operadoras()

    def _montar_operadoras(self) -> None:
        colunas_base = ["cnpj", "razao_social", "registro_ans", "modalidade", "uf"]

        if not self.df_cadastro.empty:
            cadastro = self.df_cadastro.copy()
            cadastro["cnpj"] = cadastro["cnpj"].astype(str).apply(limpar_cnpj)
            colunas_disponiveis = [col for col in colunas_base if col in cadastro.columns]
            base = cadastro[colunas_disponiveis].copy()
            for col in colunas_base:
                if col not in base.columns:
                    base[col] = None
            base = base[colunas_base]
        elif not self.df_despesas.empty:
            base = (
                self.df_despesas[["cnpj", "razao_social"]]
                .dropna()
                .drop_duplicates(subset=["cnpj"])
                .copy()
            )
            base["cnpj"] = base["cnpj"].astype(str).apply(limpar_cnpj)
            base["registro_ans"] = None
            base["modalidade"] = None
            base["uf"] = None
            base = base[colunas_base]
        else:
            base = pd.DataFrame(columns=colunas_base)

        if not self.df_despesas.empty and "razao_social" in base.columns:
            razao_por_cnpj = (
                self.df_despesas[["cnpj", "razao_social"]]
                .dropna()
                .drop_duplicates(subset=["cnpj"])
                .set_index("cnpj")["razao_social"]
                .to_dict()
            )
            base["razao_social"] = base.apply(
                lambda row: row["razao_social"]
                if pd.notna(row["razao_social"])
                else razao_por_cnpj.get(row["cnpj"]),
                axis=1,
            )

        if "cnpj" in base.columns:
            base["cnpj"] = base["cnpj"].astype(str).apply(limpar_cnpj)
        self.df_operadoras = base.drop_duplicates(subset=["cnpj"]).reset_index(drop=True)

    def listar_operadoras(self, page: int, limit: int, search: Optional[str]) -> dict:
        df = self.df_operadoras.copy()
        if not df.empty:
            df["cnpj"] = df["cnpj"].astype(str).apply(limpar_cnpj)
            df["tem_despesas"] = False
            if "razao_social" in df.columns:
                df["razao_norm"] = df["razao_social"].fillna("").apply(_normalizar_texto)
            if self.totais_por_cnpj:
                df["total_cnpj"] = df["cnpj"].apply(
                    lambda c: float(self.totais_por_cnpj.get(c, 0))
                )
                df["tem_despesas"] = df["total_cnpj"] > 0
            if self.totais_por_razao and "razao_norm" in df.columns:
                df["total_razao"] = df["razao_norm"].apply(
                    lambda r: float(self.totais_por_razao.get(r, 0))
                )
                df["tem_despesas"] = df["tem_despesas"] | (df["total_razao"] > 0)
        if search:
            search_norm = limpar_cnpj(search)
            mask_cnpj = (
                df["cnpj"].str.contains(search_norm, na=False)
                if search_norm
                else pd.Series([False] * len(df), index=df.index)
            )
            mask_razao = df["razao_social"].str.contains(search, case=False, na=False)
            df = df[mask_cnpj | mask_razao]

        total = len(df)
        if "tem_despesas" in df.columns:
            df = df.sort_values(["tem_despesas", "razao_social"], ascending=[False, True])
        offset = (page - 1) * limit
        dados = df.iloc[offset : offset + limit]

        return {
            "data": dados.fillna("").to_dict(orient="records"),
            "total": int(total),
            "page": page,
            "limit": limit,
        }

    def obter_operadora(self, cnpj: str) -> Optional[dict]:
        cnpj = limpar_cnpj(cnpj)
        df = self.df_operadoras[self.df_operadoras["cnpj"] == cnpj]
        if df.empty:
            return None
        return df.iloc[0].fillna("").to_dict()

    def obter_historico(self, cnpj: str) -> list[dict]:
        cnpj = limpar_cnpj(cnpj)
        df = self.df_despesas[self.df_despesas["cnpj"] == cnpj]
        if df.empty and "razao_social" in self.df_operadoras.columns:
            operadora = self.df_operadoras[self.df_operadoras["cnpj"] == cnpj]
            if not operadora.empty:
                razao_norm = _normalizar_texto(operadora.iloc[0]["razao_social"])
                if razao_norm:
                    df = self.df_despesas[
                        self.df_despesas["razao_social"]
                        .fillna("")
                        .apply(_normalizar_texto)
                        .eq(razao_norm)
                    ]
        if df.empty:
            return []
        agrupado = (
            df.groupby(["ano", "trimestre"])["valor_despesas"]
            .sum()
            .reset_index()
            .sort_values(["ano", "trimestre"])
        )
        if (agrupado["valor_despesas"] <= 0).all():
            return []
        return [
            {
                "ano": int(row["ano"]),
                "trimestre": int(row["trimestre"]),
                "valor_total": float(row["valor_despesas"]),
            }
            for _, row in agrupado.iterrows()
        ]

    def _calcular_estatisticas(self) -> dict:
        total_despesas = float(self.df_despesas["valor_despesas"].sum()) if not self.df_despesas.empty else 0.0
        media_despesas = float(self.df_despesas["valor_despesas"].mean()) if not self.df_despesas.empty else 0.0
        total_operadoras = int(self.df_operadoras["cnpj"].nunique()) if not self.df_operadoras.empty else 0
        media_por_operadora = (total_despesas / total_operadoras) if total_operadoras > 0 else 0.0

        mapa_uf_por_cnpj: dict[str, str] = {}
        mapa_uf_por_razao: dict[str, str] = {}
        if not self.df_cadastro.empty and "uf" in self.df_cadastro.columns:
            cadastro_validos = self.df_cadastro[["cnpj", "razao_social", "uf"]].copy()
            cadastro_validos["razao_norm"] = cadastro_validos["razao_social"].apply(_normalizar_texto)
            mapa_uf_por_cnpj = (
                cadastro_validos.dropna(subset=["cnpj"])
                .set_index("cnpj")["uf"]
                .to_dict()
            )
            mapa_uf_por_razao = (
                cadastro_validos.dropna(subset=["razao_norm"])
                .drop_duplicates(subset=["razao_norm"])
                .set_index("razao_norm")["uf"]
                .to_dict()
            )

        top_5 = []
        top_1_operadora: dict | None = None
        if not self.df_despesas.empty:
            agrupado = (
                self.df_despesas.groupby(["cnpj", "razao_social"])["valor_despesas"]
                .sum()
                .reset_index()
                .rename(columns={"valor_despesas": "total_despesas"})
            )
            agrupado["uf"] = agrupado["cnpj"].map(mapa_uf_por_cnpj)
            if "razao_social" in agrupado.columns:
                agrupado["razao_norm"] = agrupado["razao_social"].apply(_normalizar_texto)
                agrupado["uf"] = agrupado["uf"].fillna(
                    agrupado["razao_norm"].map(mapa_uf_por_razao)
                )
            agrupado["uf"] = agrupado["uf"].fillna("NÃO_INFORMADO")

            top = agrupado.sort_values("total_despesas", ascending=False).head(5)
            top_1 = top.head(1)
            top_5 = [
                {
                    "razao_social": row["razao_social"],
                    "uf": row.get("uf"),
                    "total_despesas": float(row["total_despesas"]),
                }
                for _, row in top.iterrows()
            ]
            if not top_1.empty:
                row = top_1.iloc[0]
                top_1_operadora = {
                    "razao_social": row["razao_social"],
                    "uf": row.get("uf"),
                    "total_despesas": float(row["total_despesas"]),
                }

        despesas_por_uf = []
        if not self.df_despesas.empty:
            df = self.df_despesas.copy()
            df["uf"] = df["cnpj"].map(mapa_uf_por_cnpj)
            if "razao_social" in df.columns:
                df["razao_norm"] = df["razao_social"].apply(_normalizar_texto)
                df["uf"] = df["uf"].fillna(df["razao_norm"].map(mapa_uf_por_razao))
            df["uf"] = df["uf"].fillna("NÃO_INFORMADO")

            despesas_uf = (
                df.groupby("uf")["valor_despesas"]
                .sum()
                .reset_index()
                .rename(columns={"valor_despesas": "total_despesas"})
            )

            if not self.df_cadastro.empty and "cnpj" in self.df_cadastro.columns:
                cadastro_uf = (
                    self.df_cadastro[["cnpj", "uf"]]
                    .copy()
                    .assign(uf=lambda d: d["uf"].fillna("NÃO_INFORMADO"))
                    .groupby("uf")["cnpj"]
                    .nunique()
                    .reset_index()
                    .rename(columns={"cnpj": "total_operadoras"})
                )
            else:
                cadastro_uf = (
                    df.groupby("uf")["cnpj"]
                    .nunique()
                    .reset_index()
                    .rename(columns={"cnpj": "total_operadoras"})
                )

            agregado = despesas_uf.merge(cadastro_uf, on="uf", how="left")
            agregado["total_operadoras"] = agregado["total_operadoras"].fillna(0).astype(int)
            agregado["media_por_operadora"] = agregado.apply(
                lambda row: (row["total_despesas"] / row["total_operadoras"])
                if row["total_operadoras"] > 0
                else 0.0,
                axis=1,
            )
            agregado = agregado.sort_values("total_despesas", ascending=False)

            despesas_por_uf = [
                {
                    "uf": row["uf"],
                    "total_despesas": float(row["total_despesas"]),
                    "media_por_operadora": float(row["media_por_operadora"]),
                    "total_operadoras": int(row["total_operadoras"]),
                }
                for _, row in agregado.iterrows()
            ]
        elif not self.df_agregado.empty and "uf" in self.df_agregado.columns:
            df = self.df_agregado.copy()
            df["uf"] = df["uf"].fillna("NÃO_INFORMADO")
            agregado = (
                df.groupby("uf")["total_despesas"]
                .sum()
                .reset_index()
                .rename(columns={"total_despesas": "total_despesas"})
            )
            agregado["total_operadoras"] = 0
            agregado["media_por_operadora"] = 0.0
            agregado = agregado.sort_values("total_despesas", ascending=False)
            despesas_por_uf = [
                {
                    "uf": row["uf"],
                    "total_despesas": float(row["total_despesas"]),
                    "media_por_operadora": float(row["media_por_operadora"]),
                    "total_operadoras": int(row["total_operadoras"]),
                }
                for _, row in agregado.iterrows()
            ]

        uf_lider = None
        if despesas_por_uf:
            uf_lider = next(
                (item for item in despesas_por_uf if item["uf"] != "NÃO_INFORMADO"),
                despesas_por_uf[0],
            )

        alta_variabilidade = 0
        if not self.df_agregado.empty and "alta_variabilidade" in self.df_agregado.columns:
            alta_variabilidade = int(
                self.df_agregado["alta_variabilidade"]
                .fillna(False)
                .astype(bool)
                .sum()
            )

        return {
            "total_despesas": total_despesas,
            "media_despesas": media_despesas,
            "media_por_operadora": float(media_por_operadora),
            "total_operadoras": total_operadoras,
            "top_1_operadora": top_1_operadora,
            "top_5_operadoras": top_5,
            "despesas_por_uf": despesas_por_uf,
            "uf_lider": uf_lider,
            "operadoras_alta_variabilidade": alta_variabilidade,
        }

    def obter_estatisticas(self) -> dict:
        agora = datetime.now(timezone.utc)
        if self._cache_stats and self._cache_stats.expira_em > agora:
            return self._cache_stats.valor

        valor = self._calcular_estatisticas()
        expira_em = agora + timedelta(seconds=self.settings.api_cache_ttl_seconds)
        self._cache_stats = CacheItem(valor=valor, expira_em=expira_em)
        return valor

