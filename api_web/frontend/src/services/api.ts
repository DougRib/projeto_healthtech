import type {
  DespesaTrimestre,
  EstatisticasResposta,
  OperadoraResumo,
  OperadorasPaginadas,
} from '../types'

const API_BASE_URL = import.meta.env.VITE_API_BASE_URL || 'http://localhost:8000'

async function request<T>(url: string): Promise<T> {
  const response = await fetch(url)
  if (!response.ok) {
    const message = await response.text()
    throw new Error(message || 'Erro na requisicao')
  }
  return (await response.json()) as T
}

export async function getOperadoras(
  page: number,
  limit: number,
  search?: string,
): Promise<OperadorasPaginadas> {
  const params = new URLSearchParams({
    page: String(page),
    limit: String(limit),
  })
  if (search) {
    params.set('search', search)
  }
  return request<OperadorasPaginadas>(`${API_BASE_URL}/api/operadoras?${params}`)
}

export async function getOperadora(cnpj: string): Promise<OperadoraResumo> {
  return request<OperadoraResumo>(`${API_BASE_URL}/api/operadoras/${cnpj}`)
}

export async function getDespesas(cnpj: string): Promise<DespesaTrimestre[]> {
  return request<DespesaTrimestre[]>(`${API_BASE_URL}/api/operadoras/${cnpj}/despesas`)
}

export async function getEstatisticas(): Promise<EstatisticasResposta> {
  return request<EstatisticasResposta>(`${API_BASE_URL}/api/estatisticas`)
}
