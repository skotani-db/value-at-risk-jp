import { useState, useEffect, useRef } from 'react'
import { LineChart, Line, XAxis, YAxis, CartesianGrid, Tooltip, ResponsiveContainer, Legend } from 'recharts'

export default function ETL() {
  const [portfolio, setPortfolio] = useState([])
  const [tickers, setTickers] = useState([])
  const [chartTicker, setChartTicker] = useState('')
  const [chartData, setChartData] = useState(null)
  const [filterStart, setFilterStart] = useState('2024-05-01')
  const [filterEnd, setFilterEnd] = useState('2026-05-01')

  // Refresh data (rarely used - for initial setup only)
  const [refreshing, setRefreshing] = useState(false)
  const [progress, setProgress] = useState(null)
  const pollRef = useRef(null)

  useEffect(() => {
    fetch('/api/portfolio').then(r => r.json()).then(setPortfolio).catch(() => {})
    loadTickers()
  }, [])

  const loadTickers = () => {
    fetch('/api/etl/tickers').then(r => r.json()).then(d => {
      if (d.rows?.length > 0) {
        setTickers(d.rows)
        if (!chartTicker) setChartTicker(d.rows[0].ticker)
      }
    }).catch(() => {})
  }

  useEffect(() => {
    if (!chartTicker) return
    const params = new URLSearchParams({ ticker: chartTicker, start_date: filterStart, end_date: filterEnd, limit: '2000' })
    fetch(`/api/etl/stocks?${params}`).then(r => r.json()).then(d => {
      if (d.rows) setChartData(d.rows.map(r => ({
        date: r.date?.substring(0, 10), close: parseFloat(r.close),
        open: parseFloat(r.open), high: parseFloat(r.high), low: parseFloat(r.low),
      })))
    }).catch(() => {})
  }, [chartTicker, filterStart, filterEnd])

  // Refresh (yfinance job)
  const refreshData = async () => {
    setRefreshing(true); setProgress(null)
    try {
      await fetch('/api/etl/generate', {
        method: 'POST', headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ start_date: filterStart, end_date: filterEnd })
      })
      pollRef.current = setInterval(async () => {
        const p = await fetch('/api/etl/progress').then(r => r.json())
        setProgress(p)
        if (!p.running) {
          clearInterval(pollRef.current); pollRef.current = null
          setRefreshing(false)
          if (p.done) loadTickers()
        }
      }, 5000)
    } catch (e) { setRefreshing(false) }
  }

  useEffect(() => () => { if (pollRef.current) clearInterval(pollRef.current) }, [])

  // Group portfolio by country
  const byCountry = {}
  portfolio.forEach(p => { if (!byCountry[p.country]) byCountry[p.country] = []; byCountry[p.country].push(p) })

  return (
    <div>
      <div className="page-header">
        <h1>01 - Data ETL</h1>
        <p>Yahoo Finance market data and interactive visualization</p>
      </div>

      {/* Portfolio Overview */}
      <div className="card">
        <div className="card-header">
          <div className="card-title">Portfolio ({portfolio.length} tickers)</div>
          <button className="btn btn-outline btn-sm" onClick={refreshData} disabled={refreshing}>
            {refreshing ? 'Downloading...' : 'Refresh from yfinance'}
          </button>
        </div>
        {refreshing && progress && (
          <div style={{ marginBottom: 12 }}>
            <div style={{ display: 'flex', justifyContent: 'space-between', marginBottom: 4 }}>
              <span style={{ fontSize: 12, color: 'var(--text-secondary)' }}>{progress.current_ticker}</span>
              <span style={{ fontSize: 12, color: 'var(--text-muted)' }}>{progress.current}/{progress.total}</span>
            </div>
            <div style={{ width: '100%', height: 6, background: 'var(--bg-input)', borderRadius: 3, overflow: 'hidden' }}>
              <div style={{ width: `${progress.total > 0 ? Math.round(progress.current / progress.total * 100) : 0}%`, height: '100%', background: 'linear-gradient(90deg,var(--accent-blue),var(--accent-cyan))', borderRadius: 3, transition: 'width 0.5s ease' }} />
            </div>
            {progress.run_url && <a href={progress.run_url} target="_blank" rel="noreferrer" style={{ fontSize: 12, color: 'var(--accent-blue)', marginTop: 4, display: 'inline-block' }}>View Job Run</a>}
          </div>
        )}
        <div style={{ display: 'grid', gridTemplateColumns: 'repeat(auto-fill, minmax(280px, 1fr))', gap: 12 }}>
          {Object.entries(byCountry).map(([country, stocks]) => (
            <div key={country} style={{ background: 'var(--bg-input)', borderRadius: 8, padding: 12 }}>
              <div style={{ fontSize: 11, fontWeight: 700, color: 'var(--text-muted)', textTransform: 'uppercase', letterSpacing: 0.5, marginBottom: 8 }}>{country}</div>
              {stocks.map(s => (
                <div key={s.ticker}
                  onClick={() => setChartTicker(s.ticker)}
                  style={{ display: 'flex', alignItems: 'center', gap: 8, padding: '4px 0', cursor: 'pointer', fontSize: 13 }}>
                  <span className={`badge ${s.ticker === chartTicker ? 'badge-blue' : 'badge-green'}`} style={{ minWidth: 42, textAlign: 'center' }}>{s.ticker}</span>
                  <span style={{ color: 'var(--text-secondary)', fontSize: 12, overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap' }}>{s.company}</span>
                </div>
              ))}
            </div>
          ))}
        </div>
      </div>

      {/* Price Chart */}
      {tickers.length > 0 && (
        <div className="card">
          <div className="card-header">
            <div className="card-title">Price Chart</div>
            <div style={{ display: 'flex', gap: 8, alignItems: 'center' }}>
              <select className="form-select" style={{ width: 120 }} value={chartTicker} onChange={e => setChartTicker(e.target.value)}>
                {tickers.map(t => <option key={t.ticker} value={t.ticker}>{t.ticker}</option>)}
              </select>
              <input type="date" className="form-input" style={{ width: 150 }} value={filterStart} onChange={e => setFilterStart(e.target.value)} />
              <span style={{ color: 'var(--text-muted)' }}>~</span>
              <input type="date" className="form-input" style={{ width: 150 }} value={filterEnd} onChange={e => setFilterEnd(e.target.value)} />
            </div>
          </div>
          {chartData ? (
            <ResponsiveContainer width="100%" height={400}>
              <LineChart data={chartData}>
                <CartesianGrid strokeDasharray="3 3" stroke="#2a2a4a" />
                <XAxis dataKey="date" stroke="#606080" tick={{ fontSize: 11 }} />
                <YAxis stroke="#606080" tick={{ fontSize: 11 }} domain={['auto', 'auto']} />
                <Tooltip contentStyle={{ background: '#1a1a2e', border: '1px solid #2a2a4a', borderRadius: 8 }} labelStyle={{ color: '#e8e8f0' }} />
                <Legend />
                <Line type="monotone" dataKey="close" stroke="#4f8cff" strokeWidth={2} dot={false} name="Close" />
                <Line type="monotone" dataKey="high" stroke="#22c55e" strokeWidth={1} dot={false} name="High" opacity={0.4} />
                <Line type="monotone" dataKey="low" stroke="#ef4444" strokeWidth={1} dot={false} name="Low" opacity={0.4} />
              </LineChart>
            </ResponsiveContainer>
          ) : <div className="loading"><div className="spinner" />Loading...</div>}
        </div>
      )}

      {/* Data Summary */}
      {tickers.length > 0 && (
        <div className="card">
          <div className="card-header">
            <div className="card-title">Data Summary</div>
            <div className="card-subtitle">{tickers.length} tickers</div>
          </div>
          <div style={{ overflowX: 'auto' }}>
            <table className="data-table">
              <thead><tr><th>Ticker</th><th>Records</th><th>Start</th><th>End</th><th>Avg Close</th></tr></thead>
              <tbody>
                {tickers.map(r => (
                  <tr key={r.ticker} onClick={() => setChartTicker(r.ticker)} style={{ cursor: 'pointer' }}>
                    <td><span className={`badge ${r.ticker === chartTicker ? 'badge-blue' : 'badge-green'}`}>{r.ticker}</span></td>
                    <td>{parseInt(r.cnt).toLocaleString()}</td>
                    <td>{r.min_date?.substring(0, 10)}</td>
                    <td>{r.max_date?.substring(0, 10)}</td>
                    <td>{r.avg_close}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </div>
      )}

      {tickers.length === 0 && !refreshing && (
        <div className="card" style={{ textAlign: 'center', padding: 40, color: 'var(--text-muted)' }}>
          No market data yet. Click "Refresh from yfinance" to download.
        </div>
      )}
    </div>
  )
}
