import { useState, useEffect } from 'react'
import { LineChart, Line, XAxis, YAxis, CartesianGrid, Tooltip, ResponsiveContainer, Legend } from 'recharts'

const NODE_TYPES = {
  source: { label: 'Source', color: '#06b6d4' },
  config: { label: 'Config', color: '#a855f7' },
  transform: { label: 'Transform', color: '#f97316' },
  table: { label: 'Delta Table', color: '#22c55e' },
}

export default function ETL() {
  const [lineage, setLineage] = useState(null)
  const [summary, setSummary] = useState(null)
  const [stockData, setStockData] = useState(null)
  const [selectedTicker, setSelectedTicker] = useState('')
  const [tickers, setTickers] = useState([])
  const [loading, setLoading] = useState(true)

  useEffect(() => {
    Promise.all([
      fetch('/api/etl/lineage').then(r => r.json()),
      fetch('/api/etl/stocks/summary').then(r => r.json()),
    ]).then(([lin, sum]) => {
      setLineage(lin)
      setSummary(sum)
      if (sum.rows && sum.rows.length > 0) {
        const t = sum.rows.map(r => r.ticker)
        setTickers(t)
        setSelectedTicker(t[0])
      }
      setLoading(false)
    }).catch(() => setLoading(false))
  }, [])

  useEffect(() => {
    if (!selectedTicker) return
    fetch(`/api/etl/stocks?ticker=${selectedTicker}&limit=500`)
      .then(r => r.json())
      .then(data => {
        if (data.rows) {
          setStockData(data.rows.reverse().map(r => ({
            ...r,
            date: r.date?.substring(0, 10),
            close: parseFloat(r.close),
            open: parseFloat(r.open),
            high: parseFloat(r.high),
            low: parseFloat(r.low),
          })))
        }
      })
  }, [selectedTicker])

  if (loading) return <div className="loading"><div className="spinner" /> Loading...</div>

  // Build lineage layers
  const layers = [
    { label: 'Sources', nodes: lineage?.nodes?.filter(n => n.type === 'source' || n.type === 'config') },
    { label: 'Transforms', nodes: lineage?.nodes?.filter(n => n.type === 'transform') },
    { label: 'Delta Tables', nodes: lineage?.nodes?.filter(n => n.type === 'table') },
  ]

  return (
    <div>
      <div className="page-header">
        <h1>01 - Data ETL</h1>
        <p>Market data ingestion, validation, and transformation pipeline</p>
      </div>

      {/* Lineage */}
      <div className="card">
        <div className="card-header">
          <div className="card-title">Data Lineage</div>
          <div className="card-subtitle">ETL pipeline flow</div>
        </div>
        <div className="lineage-container">
          {layers.map((layer, li) => (
            <div key={li}>
              <div style={{ fontSize: 11, fontWeight: 600, color: 'var(--text-muted)', textTransform: 'uppercase', letterSpacing: 1, marginBottom: 8 }}>
                {layer.label}
              </div>
              <div className="lineage-row">
                {layer.nodes?.map((node, ni) => (
                  <div key={node.id}>
                    <div className={`lineage-node ${node.type}`}>
                      <span style={{
                        width: 8, height: 8, borderRadius: 4,
                        background: NODE_TYPES[node.type]?.color,
                        display: 'inline-block',
                      }} />
                      {node.label}
                    </div>
                  </div>
                ))}
              </div>
              {li < layers.length - 1 && (
                <div style={{ textAlign: 'center', color: 'var(--text-muted)', margin: '8px 0' }}>
                  {'|'.repeat(3)}
                </div>
              )}
            </div>
          ))}
        </div>
      </div>

      {/* Stock Summary */}
      <div className="card">
        <div className="card-header">
          <div className="card-title">Stock Data Summary</div>
          <div className="card-subtitle">{summary?.rows?.length || 0} tickers</div>
        </div>
        <div style={{ overflowX: 'auto' }}>
          <table className="data-table">
            <thead>
              <tr>
                <th>Ticker</th>
                <th>Records</th>
                <th>Start Date</th>
                <th>End Date</th>
                <th>Avg Close</th>
                <th>Std Dev</th>
              </tr>
            </thead>
            <tbody>
              {summary?.rows?.map(r => (
                <tr key={r.ticker} onClick={() => setSelectedTicker(r.ticker)} style={{ cursor: 'pointer' }}>
                  <td>
                    <span className={`badge ${r.ticker === selectedTicker ? 'badge-blue' : 'badge-green'}`}>
                      {r.ticker}
                    </span>
                  </td>
                  <td>{parseInt(r.record_count).toLocaleString()}</td>
                  <td>{r.min_date?.substring(0, 10)}</td>
                  <td>{r.max_date?.substring(0, 10)}</td>
                  <td>{r.avg_close}</td>
                  <td>{r.std_close}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      </div>

      {/* Stock Chart */}
      {stockData && (
        <div className="card">
          <div className="card-header">
            <div className="card-title">Price Chart: {selectedTicker}</div>
            <select
              className="form-select"
              style={{ width: 'auto', minWidth: 120 }}
              value={selectedTicker}
              onChange={e => setSelectedTicker(e.target.value)}
            >
              {tickers.map(t => <option key={t} value={t}>{t}</option>)}
            </select>
          </div>
          <ResponsiveContainer width="100%" height={350}>
            <LineChart data={stockData}>
              <CartesianGrid strokeDasharray="3 3" stroke="#2a2a4a" />
              <XAxis dataKey="date" stroke="#606080" tick={{ fontSize: 11 }} />
              <YAxis stroke="#606080" tick={{ fontSize: 11 }} />
              <Tooltip
                contentStyle={{ background: '#1a1a2e', border: '1px solid #2a2a4a', borderRadius: 8 }}
                labelStyle={{ color: '#e8e8f0' }}
              />
              <Legend />
              <Line type="monotone" dataKey="close" stroke="#4f8cff" strokeWidth={2} dot={false} name="Close" />
              <Line type="monotone" dataKey="high" stroke="#22c55e" strokeWidth={1} dot={false} name="High" opacity={0.5} />
              <Line type="monotone" dataKey="low" stroke="#ef4444" strokeWidth={1} dot={false} name="Low" opacity={0.5} />
            </LineChart>
          </ResponsiveContainer>
        </div>
      )}
    </div>
  )
}
