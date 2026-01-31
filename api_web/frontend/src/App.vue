<script setup lang="ts">
import { computed, onMounted, ref } from 'vue'
import OperadorasTable from './components/OperadorasTable.vue'
import OperadoraDetalhe from './components/OperadoraDetalhe.vue'
import DespesasChart from './components/DespesasChart.vue'
import { getDespesas, getEstatisticas, getOperadora, getOperadoras } from './services/api'
import type { DespesaTrimestre, EstatisticasResposta, OperadoraResumo } from './types'
import { formatCurrency, formatNumber } from './utils/format'

const operadoras = ref<OperadoraResumo[]>([])
const total = ref(0)
const page = ref(1)
const limit = ref(10)
const search = ref('')
const loadingOperadoras = ref(false)
const errorOperadoras = ref('')

const selectedOperadora = ref<OperadoraResumo | null>(null)
const historico = ref<DespesaTrimestre[]>([])
const loadingDetalhe = ref(false)
const errorDetalhe = ref('')

const estatisticas = ref<EstatisticasResposta | null>(null)
const loadingStats = ref(false)
const errorStats = ref('')

const topOperadoras = computed(() => estatisticas.value?.top_5_operadoras ?? [])
const top1Operadora = computed(() => estatisticas.value?.top_1_operadora)
const ufLider = computed(() => estatisticas.value?.uf_lider)

async function carregarOperadoras() {
  loadingOperadoras.value = true
  errorOperadoras.value = ''
  try {
    const resposta = await getOperadoras(page.value, limit.value, search.value || undefined)
    operadoras.value = resposta.data
    total.value = resposta.total
  } catch (err: any) {
    errorOperadoras.value = err.message || 'Erro ao carregar operadoras'
  } finally {
    loadingOperadoras.value = false
  }
}

async function carregarDetalhes(cnpj: string) {
  loadingDetalhe.value = true
  errorDetalhe.value = ''
  try {
    const [op, despesas] = await Promise.all([getOperadora(cnpj), getDespesas(cnpj)])
    selectedOperadora.value = op
    historico.value = despesas
  } catch (err: any) {
    errorDetalhe.value = err.message || 'Erro ao carregar detalhes'
  } finally {
    loadingDetalhe.value = false
  }
}

async function carregarEstatisticas() {
  loadingStats.value = true
  errorStats.value = ''
  try {
    estatisticas.value = await getEstatisticas()
  } catch (err: any) {
    errorStats.value = err.message || 'Erro ao carregar estatisticas'
  } finally {
    loadingStats.value = false
  }
}

function atualizarPagina(novaPagina: number) {
  page.value = novaPagina
  carregarOperadoras()
}

function atualizarBusca(valor: string) {
  search.value = valor
}

function executarBusca() {
  page.value = 1
  carregarOperadoras()
}

onMounted(() => {
  carregarOperadoras()
  carregarEstatisticas()
})
</script>

<template>
  <header class="page-header">
    <h1>Operadoras de Saúde - Dashboard</h1>
    <p>Consulta de operadoras, histórico de despesas e distribuição por UF.</p>
  </header>

  <section class="card summary-card">
    <h2>Resumo</h2>
    <div v-if="errorStats" class="alert">{{ errorStats }}</div>
    <p v-else-if="loadingStats" class="muted">Carregando estatísticas...</p>
    <div v-else-if="estatisticas" class="summary-grid">
      <div class="stat-cards">
        <div class="stat-card">
          <span class="stat-label">Total de despesas</span>
          <span class="stat-value">{{ formatCurrency(estatisticas.total_despesas) }}</span>
        </div>
        <div class="stat-card">
          <span class="stat-label">Média por operadora</span>
          <span class="stat-value">{{ formatCurrency(estatisticas.media_por_operadora) }}</span>
        </div>
        <div class="stat-card">
          <span class="stat-label">Top 1 operadora</span>
          <span class="stat-value">
            {{ top1Operadora ? formatCurrency(top1Operadora.total_despesas) : '-' }}
          </span>
          <span class="stat-subtitle">
            {{ top1Operadora?.razao_social || 'Sem dados' }}
          </span>
        </div>
        <div class="stat-card">
          <span class="stat-label">UF líder em despesas</span>
          <span class="stat-value">
            {{ ufLider ? formatCurrency(ufLider.total_despesas) : '-' }}
          </span>
          <span class="stat-subtitle">{{ ufLider?.uf || 'Sem dados' }}</span>
        </div>
        <div class="stat-card">
          <span class="stat-label">Operadoras com alta variabilidade</span>
          <span class="stat-value">
            {{ formatNumber(estatisticas.operadoras_alta_variabilidade) }}
          </span>
        </div>
      </div>
      <div class="summary-panel">
        <p class="summary-title">Top 5 operadoras por despesas</p>
        <table v-if="topOperadoras.length" class="table">
          <thead>
            <tr>
              <th>Operadora</th>
              <th>UF</th>
              <th class="numeric">Total</th>
            </tr>
          </thead>
          <tbody>
            <tr v-for="item in topOperadoras" :key="item.razao_social">
              <td class="truncate" :title="item.razao_social">{{ item.razao_social }}</td>
              <td>{{ item.uf || '-' }}</td>
              <td class="numeric">{{ formatCurrency(item.total_despesas) }}</td>
            </tr>
          </tbody>
        </table>
        <div v-else class="empty-state">Sem dados para exibir.</div>
      </div>
    </div>
    <div v-else class="empty-state">Nenhuma estatistica disponivel.</div>
  </section>

  <section class="grid">
    <OperadorasTable
      :items="operadoras"
      :loading="loadingOperadoras"
      :error="errorOperadoras"
      :page="page"
      :limit="limit"
      :total="total"
      :search="search"
      @update:page="atualizarPagina"
      @update:search="atualizarBusca"
      @refresh="executarBusca"
      @select="carregarDetalhes"
    />

    <OperadoraDetalhe
      :operadora="selectedOperadora"
      :historico="historico"
      :loading="loadingDetalhe"
      :error="errorDetalhe"
    />
  </section>

  <DespesasChart
    v-if="estatisticas"
    :dados="estatisticas.despesas_por_uf"
  />
</template>
