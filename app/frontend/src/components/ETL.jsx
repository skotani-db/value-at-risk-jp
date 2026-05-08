import { useState, useEffect, useRef } from 'react'
import { LineChart, Line, XAxis, YAxis, CartesianGrid, Tooltip, ResponsiveContainer, Legend } from 'recharts'

export default function ETL() {
  const [portfolio, setPortfolio] = useState([])
  const [selectedTickers, setSelectedTickers] = useState(new Set())
  const [startDate, setStartDate] = useState('2024-05-01')
  const [endDate, setEndDate] = useState('2026-05-01')
  const [generating, setGenerating] = useState(false)
  const [progress, setProgress] = useState(null)
  const [genError, setGenError] = useState(null)
  const pollRef = useRef(null)

  const [tickers, setTickers] = useState([])
  const [chartTicker, setChartTicker] = useState('')
  const [chartData, setChartData] = useState(null)
  const [filterStart, setFilterStart] = useState('2024-05-01')
  const [filterEnd, setFilterEnd] = useState('2026-05-01')
  const [generated, setGenerated] = useState(false)

  useEffect(() => {
    fetch('/api/portfolio').then(r => r.json()).then(p => {
      setPortfolio(p)
      setSelectedTickers(new Set(p.map(x => x.ticker)))
    }).catch(() => {})
    fetch('/api/etl/tickers').then(r => r.json()).then(d => {
      if (d.rows && d.rows.length > 0) {
        setTickers(d.rows); setChartTicker(d.rows[0].ticker); setGenerated(true)
      }
    }).catch(() => {})
  }, [])

  const toggleTicker = (t) => {
    setSelectedTickers(prev => {
      const next = new Set(prev)
      next.has(t) ? next.delete(t) : next.add(t)
      return next
    })
  }
  const selectAll = () => setSelectedTickers(new Set(portfolio.map(x => x.ticker)))
  const selectNone = () => setSelectedTickers(new Set())

  const pollProgress = () => {
    pollRef.current = setInterval(async () => {
      try {
        const p = await fetch('/api/etl/progress').then(r => r.json())
        setProgress(p)
        if (!p.running) {
          clearInterval(pollRef.current)
          pollRef.current = null
          setGenerating(false)
          if (p.error) {
            setGenError(p.error)
          } else if (p.done) {
            const t = await fetch('/api/etl/tickers').then(r => r.json())
            if (t.rows) { setTickers(t.rows); setChartTicker(t.rows[0].ticker); setGenerated(true) }
          }
        }
      } catch(e) {}
    }, 1500)
  }

  useEffect(() => { return () => { if (pollRef.current) clearInterval(pollRef.current) } }, [])

  const generate = async () => {
    setGenerating(true); setGenError(null); setProgress(null)
    try {
      const r = await fetch('/api/etl/generate', {
        method:'POST', headers:{'Content-Type':'application/json'},
        body: JSON.stringify({ tickers: [...selectedTickers], start_date: startDate, end_date: endDate })
      })
      if (!r.ok) { const e = await r.text(); throw new Error(e) }
      pollProgress()
    } catch(e) { setGenError(e.message); setGenerating(false) }
  }

  useEffect(() => {
    if (!chartTicker || !generated) return
    const params = new URLSearchParams({ ticker: chartTicker, start_date: filterStart, end_date: filterEnd, limit: '2000' })
    fetch(`/api/etl/stocks?${params}`).then(r => r.json()).then(d => {
      if (d.rows) setChartData(d.rows.map(r => ({
        date: r.date?.substring(0,10), close: parseFloat(r.close),
        open: parseFloat(r.open), high: parseFloat(r.high), low: parseFloat(r.low),
      })))
    }).catch(() => {})
  }, [chartTicker, filterStart, filterEnd, generated])

  const byCountry = {}
  portfolio.forEach(p => { if (!byCountry[p.country]) byCountry[p.country] = []; byCountry[p.country].push(p) })

  const pct = progress && progress.total > 0 ? Math.round(progress.current / progress.total * 100) : 0

  return (
    <div>
      <div className="page-header">
        <h1>01 - Data ETL</h1>
        <p>Market data generation and interactive visualization</p>
      </div>

      {/* Portfolio Selection */}
      <div className="card">
        <div className="card-header">
          <div className="card-title">Portfolio Selection</div>
          <div style={{display:'flex',gap:8}}>
            <button className="btn btn-outline btn-sm" onClick={selectAll}>Select All</button>
            <button className="btn btn-outline btn-sm" onClick={selectNone}>Clear</button>
            <span style={{fontSize:13,color:'var(--text-muted)',alignSelf:'center'}}>{selectedTickers.size} / {portfolio.length}</span>
          </div>
        </div>
        <div style={{display:'grid',gridTemplateColumns:'repeat(auto-fill, minmax(280px, 1fr))',gap:12}}>
          {Object.entries(byCountry).map(([country, stocks]) => (
            <div key={country} style={{background:'var(--bg-input)',borderRadius:8,padding:12}}>
              <div style={{fontSize:11,fontWeight:700,color:'var(--text-muted)',textTransform:'uppercase',letterSpacing:0.5,marginBottom:8}}>{country}</div>
              {stocks.map(s => (
                <label key={s.ticker} style={{display:'flex',alignItems:'center',gap:8,padding:'4px 0',cursor:'pointer',fontSize:13}}>
                  <input type="checkbox" checked={selectedTickers.has(s.ticker)} onChange={()=>toggleTicker(s.ticker)}
                    style={{accentColor:'var(--accent-blue)',width:16,height:16}} />
                  <span style={{fontWeight:600,color:'var(--accent-blue)',minWidth:42}}>{s.ticker}</span>
                  <span style={{color:'var(--text-secondary)',fontSize:12,overflow:'hidden',textOverflow:'ellipsis',whiteSpace:'nowrap'}}>{s.company}</span>
                </label>
              ))}
            </div>
          ))}
        </div>
      </div>

      {/* Date Range & Generate */}
      <div className="card">
        <div className="card-header">
          <div className="card-title">Data Generation</div>
        </div>
        <div style={{display:'flex',gap:16,alignItems:'flex-end',flexWrap:'wrap'}}>
          <div className="form-group" style={{marginBottom:0}}>
            <label className="form-label">Start Date</label>
            <input type="date" className="form-input" style={{width:170}} value={startDate} onChange={e=>setStartDate(e.target.value)} />
          </div>
          <div className="form-group" style={{marginBottom:0}}>
            <label className="form-label">End Date</label>
            <input type="date" className="form-input" style={{width:170}} value={endDate} onChange={e=>setEndDate(e.target.value)} />
          </div>
          <button className="btn btn-primary" onClick={generate} disabled={generating || selectedTickers.size===0}
            style={{height:42}}>
            {generating ? 'Downloading...' : 'Download from yfinance'}
          </button>
        </div>

        {/* Progress Bar */}
        {generating && progress && (
          <div style={{marginTop:16}}>
            <div style={{display:'flex',justifyContent:'space-between',alignItems:'center',marginBottom:6}}>
              <span style={{fontSize:13,color:'var(--text-secondary)'}}>
                Downloading <strong style={{color:'var(--accent-blue)'}}>{progress.current_ticker}</strong>
              </span>
              <span style={{fontSize:13,color:'var(--text-muted)'}}>{progress.current} / {progress.total} ({pct}%)</span>
            </div>
            <div style={{width:'100%',height:8,background:'var(--bg-input)',borderRadius:4,overflow:'hidden'}}>
              <div style={{
                width:`${pct}%`, height:'100%',
                background:'linear-gradient(90deg, var(--accent-blue), var(--accent-cyan))',
                borderRadius:4, transition:'width 0.5s ease',
              }} />
            </div>
          </div>
        )}

        {/* Done */}
        {progress && progress.done && !generating && (
          <div style={{marginTop:12,padding:'10px 14px',background:'rgba(34,197,94,0.1)',border:'1px solid rgba(34,197,94,0.3)',borderRadius:8,fontSize:13,color:'var(--accent-green)'}}>
            Download complete - {progress.total - 3} tickers + market indicators
          </div>
        )}

        {genError && (
          <div style={{marginTop:12,padding:'10px 14px',background:'rgba(239,68,68,0.1)',border:'1px solid rgba(239,68,68,0.3)',borderRadius:8,fontSize:13,color:'var(--accent-red)'}}>
            Error: {genError}
          </div>
        )}
      </div>

      {/* Interactive Chart */}
      {generated && (
        <div className="card">
          <div className="card-header">
            <div className="card-title">Price Chart</div>
            <div style={{display:'flex',gap:8,alignItems:'center'}}>
              <select className="form-select" style={{width:120}} value={chartTicker} onChange={e=>setChartTicker(e.target.value)}>
                {tickers.map(t => <option key={t.ticker} value={t.ticker}>{t.ticker}</option>)}
              </select>
              <input type="date" className="form-input" style={{width:150}} value={filterStart} onChange={e=>setFilterStart(e.target.value)} />
              <span style={{color:'var(--text-muted)'}}>~</span>
              <input type="date" className="form-input" style={{width:150}} value={filterEnd} onChange={e=>setFilterEnd(e.target.value)} />
            </div>
          </div>
          {chartData ? (
            <ResponsiveContainer width="100%" height={400}>
              <LineChart data={chartData}>
                <CartesianGrid strokeDasharray="3 3" stroke="#2a2a4a" />
                <XAxis dataKey="date" stroke="#606080" tick={{fontSize:11}} />
                <YAxis stroke="#606080" tick={{fontSize:11}} domain={['auto','auto']} />
                <Tooltip contentStyle={{background:'#1a1a2e',border:'1px solid #2a2a4a',borderRadius:8}} labelStyle={{color:'#e8e8f0'}} />
                <Legend />
                <Line type="monotone" dataKey="close" stroke="#4f8cff" strokeWidth={2} dot={false} name="Close" />
                <Line type="monotone" dataKey="high" stroke="#22c55e" strokeWidth={1} dot={false} name="High" opacity={0.4} />
                <Line type="monotone" dataKey="low" stroke="#ef4444" strokeWidth={1} dot={false} name="Low" opacity={0.4} />
              </LineChart>
            </ResponsiveContainer>
          ) : <div className="loading"><div className="spinner"/>Loading...</div>}
        </div>
      )}

      {/* Ticker Summary */}
      {tickers.length > 0 && (
        <div className="card">
          <div className="card-header">
            <div className="card-title">Data Summary</div>
            <div className="card-subtitle">{tickers.length} tickers loaded</div>
          </div>
          <div style={{overflowX:'auto'}}>
            <table className="data-table">
              <thead><tr><th>Ticker</th><th>Records</th><th>Start</th><th>End</th><th>Avg Close</th></tr></thead>
              <tbody>
                {tickers.map(r => (
                  <tr key={r.ticker} onClick={()=>setChartTicker(r.ticker)} style={{cursor:'pointer'}}>
                    <td><span className={`badge ${r.ticker===chartTicker?'badge-blue':'badge-green'}`}>{r.ticker}</span></td>
                    <td>{parseInt(r.cnt).toLocaleString()}</td>
                    <td>{r.min_date?.substring(0,10)}</td>
                    <td>{r.max_date?.substring(0,10)}</td>
                    <td>{r.avg_close}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </div>
      )}
    </div>
  )
}
