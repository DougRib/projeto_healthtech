<script setup lang="ts">
import { computed } from 'vue'
import type { OperadoraResumo } from '../types'
import { formatCnpj } from '../utils/format'

const props = defineProps<{
  items: OperadoraResumo[]
  loading: boolean
  error: string
  page: number
  limit: number
  total: number
  search: string
}>()

const emit = defineEmits<{
  (event: 'select', cnpj: string): void
  (event: 'update:page', page: number): void
  (event: 'update:search', value: string): void
  (event: 'refresh'): void
}>()

const totalPages = computed(() => Math.max(1, Math.ceil(props.total / props.limit)))

function onSearchInput(event: Event) {
  emit('update:search', (event.target as HTMLInputElement).value)
}
</script>

<template>
  <div class="card">
    <h2>Operadoras</h2>
    <div class="controls">
      <input
        type="text"
        class="outline-none focus:ring-1 focus:ring-blue-500  rounded-md p-2 transition-all duration-200"
        :value="search"
        placeholder="Buscar por razão social ou CNPJ"
        @input="onSearchInput"
      />
      <button class="button primary transition-all duration-200" type="button" @click="emit('refresh')">Buscar</button>
    </div>

    <div v-if="error" class="alert">{{ error }}</div>
    <p v-else-if="loading" class="muted">Carregando operadoras...</p>

    <div v-else-if="items.length === 0" class="empty-state">Nenhuma operadora encontrada.</div>

    <table v-else class="table">
      <thead>
        <tr>
          <th>CNPJ</th>
          <th>Razão Social</th>
          <th>UF</th>
          <th>Modalidade</th>
          <th>Despesas</th>
          <th>Acões</th>
        </tr>
      </thead>
      <tbody>
        <tr v-for="item in items" :key="item.cnpj">
          <td>{{ formatCnpj(item.cnpj) }}</td>
          <td class="truncate" :title="item.razao_social">{{ item.razao_social }}</td>
          <td>{{ item.uf || '-' }}</td>
          <td>{{ item.modalidade || '-' }}</td>
          <td>
            <span class="status" :class="item.tem_despesas ? 'status--ok' : 'status--warn'">
              {{ item.tem_despesas ? 'Com despesas' : 'Sem despesas' }}
            </span>
          </td>
          <td>
            <button class="button" type="button" @click="emit('select', item.cnpj)">Detalhes</button>
          </td>
        </tr>
      </tbody>
    </table>

    <div class="pagination">
      <button
        class="button secondary"
        type="button"
        :disabled="page <= 1"
        @click="emit('update:page', page - 1)"
      >
        Anterior
      </button>
      <span class="muted">Pagina {{ page }} de {{ totalPages }}</span>
      <button
        class="button secondary"
        type="button"
        :disabled="page >= totalPages"
        @click="emit('update:page', page + 1)"
      >
        Próxima
      </button>
    </div>
  </div>
</template>
