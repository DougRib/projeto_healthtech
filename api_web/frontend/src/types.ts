export type OperadoraResumo = {
  cnpj: string
  razao_social: string
  registro_ans?: string
  modalidade?: string
  uf?: string
  tem_despesas?: boolean
}

export type OperadorasPaginadas = {
  data: OperadoraResumo[]
  total: number
  page: number
  limit: number
}

export type DespesaTrimestre = {
  ano: number
  trimestre: number
  valor_total: number
}

export type EstatisticasResposta = {
  total_despesas: number
  media_despesas: number
  media_por_operadora: number
  total_operadoras: number
  top_1_operadora?: {
    razao_social: string
    uf?: string
    total_despesas: number
  }
  top_5_operadoras: {
    razao_social: string
    uf?: string
    total_despesas: number
  }[]
  despesas_por_uf: {
    uf: string
    total_despesas: number
    media_por_operadora: number
    total_operadoras: number
  }[]
  uf_lider?: {
    uf: string
    total_despesas: number
  }
  operadoras_alta_variabilidade: number
}
