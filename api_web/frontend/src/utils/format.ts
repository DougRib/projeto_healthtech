export function formatCurrency(value: number): string {
  if (Number.isNaN(value)) return '-'
  return new Intl.NumberFormat('pt-BR', {
    style: 'currency',
    currency: 'BRL',
    maximumFractionDigits: 2,
  }).format(value)
}

export function formatNumber(value: number): string {
  if (Number.isNaN(value)) return '-'
  return new Intl.NumberFormat('pt-BR').format(value)
}

export function formatCnpj(value: string): string {
  const digits = (value || '').replace(/\D/g, '')
  if (digits.length !== 14) return value
  return `${digits.slice(0, 2)}.${digits.slice(2, 5)}.${digits.slice(5, 8)}/${digits.slice(
    8,
    12,
  )}-${digits.slice(12)}`
}

export function formatShortCurrency(value: number): string {
  if (Number.isNaN(value)) return '-'
  const abs = Math.abs(value)
  const units = [
    { label: 'bi', divisor: 1_000_000_000 },
    { label: 'mi', divisor: 1_000_000 },
    { label: 'mil', divisor: 1_000 },
  ]
  const unit = units.find((u) => abs >= u.divisor)
  if (!unit) return formatCurrency(value)
  const scaled = value / unit.divisor
  return `${scaled.toFixed(1)} ${unit.label}`
}
