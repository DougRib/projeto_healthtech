<script setup lang="ts">
import type { DespesaTrimestre, OperadoraResumo } from '../types'
import { formatCnpj, formatCurrency } from '../utils/format'

defineProps<{
  operadora: OperadoraResumo | null
  historico: DespesaTrimestre[]
  loading: boolean
  error: string
}>()
</script>

<template>
  <div class="card">
    <h2>Detalhes da Operadora</h2>
    <div v-if="error" class="alert">{{ error }}</div>
    <p v-else-if="loading" class="muted">Carregando detalhes...</p>
    <div v-else-if="operadora">
      <p><span class="pill">CNPJ</span> {{ formatCnpj(operadora.cnpj) }}</p>
      <p><span class="pill">Razao Social</span> {{ operadora.razao_social }}</p>
      <p><span class="pill">Registro ANS</span> {{ operadora.registro_ans || 'N/A' }}</p>
      <p><span class="pill">Modalidade</span> {{ operadora.modalidade || 'N/A' }}</p>
      <p><span class="pill">UF</span> {{ operadora.uf || 'N/A' }}</p>

      <h3>Histórico de Despesas</h3>
      <table v-if="historico.length" class="table">
        <thead>
          <tr>
            <th>Ano</th>
            <th>Trimestre</th>
            <th class="numeric">Total</th>
          </tr>
        </thead>
        <tbody>
          <tr v-for="item in historico" :key="`${item.ano}-${item.trimestre}`">
            <td>{{ item.ano }}</td>
            <td>{{ item.trimestre }}</td>
            <td class="numeric">{{ formatCurrency(item.valor_total) }}</td>
          </tr>
        </tbody>
      </table>
      <div v-else class="empty-state">
        Sem despesas nos 3 trimestres analisados para este CNPJ.
      </div>
    </div>
    <p v-else class="muted">Selecione uma operadora para ver detalhes.</p>
  </div>
</template>
