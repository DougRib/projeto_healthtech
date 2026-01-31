<script setup lang="ts">
import { computed, onMounted, onBeforeUnmount, ref, watch } from "vue";
import { formatNumber, formatShortCurrency } from "../utils/format";

declare const Chart: any;

interface ChartInstance {
  destroy(): void;
  update(): void;
}

type DespesaUf = {
  uf: string;
  total_despesas: number;
  media_por_operadora: number;
  total_operadoras: number;
};

type Metric = "total" | "media" | "operadoras";

const props = defineProps<{
  dados: DespesaUf[];
}>();

const canvasRef = ref<HTMLCanvasElement | null>(null);
const metric = ref<Metric>("total");
let chartInstance: ChartInstance | null = null;

const metricConfig = computed(() => {
  switch (metric.value) {
    case "media":
      return {
        label: "Média por operadora (UF)",
        key: "media_por_operadora" as const,
        type: "line",
      };
    case "operadoras":
      return {
        label: "Quantidade de operadoras por UF",
        key: "total_operadoras" as const,
        type: "bar",
      };
    default:
      return {
        label: "Total de despesas por UF",
        key: "total_despesas" as const,
        type: "bar",
      };
  }
});

const dadosOrdenados = computed(() => {
  const base = [...props.dados];
  const key = metricConfig.value.key;
  return base
    .filter((item) => item && typeof item[key] === "number")
    .sort((a, b) => (b[key] as number) - (a[key] as number))
    .slice(0, 10);
});

function formatValue(value: number) {
  if (metric.value === "operadoras") return formatNumber(value);
  return formatShortCurrency(value);
}

function renderChart() {
  if (!canvasRef.value) return;

  const labels = dadosOrdenados.value.map((item) => item.uf || "N/A");
  const key = metricConfig.value.key;
  const values = dadosOrdenados.value.map((item) => item[key]);

  if (chartInstance) {
    chartInstance.destroy();
  }

  chartInstance = new (Chart as any)(canvasRef.value, {
    type: metricConfig.value.type,
    data: {
      labels,
      datasets: [
        {
          label: metricConfig.value.label,
          data: values,
          backgroundColor: metric.value === "operadoras" ? "#a5b4fc" : "#60a5fa",
          borderColor: metric.value === "media" ? "#2563eb" : "#60a5fa",
          borderWidth: metric.value === "media" ? 2 : 0,
          tension: metric.value === "media" ? 0.3 : 0,
          fill: metric.value !== "media",
        },
      ],
    },
    options: {
      responsive: true,
      plugins: {
        legend: {
          position: "top",
        },
        tooltip: {
          callbacks: {
            label: (context: any) => ` ${formatValue(Number(context.raw))}`,
          },
        },
      },
      scales: {
        y: {
          beginAtZero: true,
          ticks: {
            callback: (value: any) => formatValue(Number(value)),
          },
        },
      },
    },
  });
}

onMounted(renderChart);
onBeforeUnmount(() => {
  if (chartInstance) chartInstance.destroy();
});

watch([dadosOrdenados, metric], () => renderChart());
</script>

<template>
  <div class="card">
    <div class="chart-header">
      <div>
        <h2>Distribuição de Despesas por UF</h2>
        <p class="muted">Top 10 UFs com a métrica selecionada.</p>
      </div>
      <div class="chart-controls">
        <label class="muted" for="metric-select">Métrica</label>
        <select id="metric-select" v-model="metric">
          <option value="total">Total de despesas</option>
          <option value="media">Média por operadora</option>
          <option value="operadoras">Qtd. de operadoras</option>
        </select>
      </div>
    </div>
    <div v-if="dadosOrdenados.length === 0" class="empty-state">
      Sem dados para gráfico.
    </div>
    <canvas v-else ref="canvasRef" height="160"></canvas>
  </div>
</template>
