import { useState, useEffect } from 'react'
import { LineChart, Line, XAxis, YAxis, CartesianGrid, Tooltip, ResponsiveContainer, Legend } from 'recharts'

const COUNTRY_COLORS = {
  CHILE:'#06b6d4', COLOMBIA:'#a855f7', MEXICO:'#f97316', PANAMA:'#22c55e', PERU:'#ef4444',
}

export default function Aggregation() {
  const [varData, setVarData] = useState(null)
  const [countryData, setCountryData] = useState(null)
  const [dashboardUrl, setDashboardUrl] = useState('')
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState(null)

  useEffect(() => {
    Promise.all([
      fetch('/api/aggregation/var_timeseries').then(r => r.json()),
      fetch('/api/aggregation/var_by_country').then(r => r.json()),
    ]).then(([vd, cd]) => {
      if (vd.rows && vd.rows.length > 0) {
        setVarData(vd.rows.map(r => ({ date: r.date?.substring(0,10), var_99: parseFloat(r.var_99), es_99: parseFloat(r.es_99 || 0) })))
      }
      if (cd.rows && cd.rows.length > 0) {
        const m = {}
        cd.rows.forEach(r => {
          const d = r.date?.substring(0,10)
          if (!m[d]) m[d] = { date: d }
          m[d][r.country] = parseFloat(r.var_99)
        })
        setCountryData(Object.values(m))
      }
      setLoading(false)
    }).catch(e => { setError(e.message); setLoading(false) })
  }, [])

  return (
    <div>
      <div className="page-header">
        <h1>04 - Aggregation</h1>
        <p>Portfolio VaR aggregation queried via SQL Warehouse</p>
      </div>

      {/* AI/BI Dashboard Embed */}
      <div className="card">
        <div className="card-header">
          <div className="card-title">AI/BI Dashboard</div>
          <input className="form-input" style={{width:450}}
            placeholder="Paste AI/BI Dashboard embed URL..."
            value={dashboardUrl} onChange={e=>setDashboardUrl(e.target.value)} />
        </div>
        {dashboardUrl ? (
          <iframe src={dashboardUrl} className="dashboard-embed" title="AI/BI Dashboard" allow="fullscreen" />
        ) : (
          <div style={{padding:50,textAlign:'center',background:'var(--bg-input)',borderRadius:8,color:'var(--text-muted)',fontSize:14}}>
            Enter your AI/BI (Lakeview) Dashboard embed URL above.<br/>
            <span style={{fontSize:12}}>Format: https://&lt;workspace&gt;/embed/dashboardsv3/...</span>
          </div>
        )}
      </div>

      {/* VaR Timeseries */}
      {varData && (
        <div className="card">
          <div className="card-header">
            <div className="card-title">Portfolio VaR99 Over Time</div>
            <div className="card-subtitle">{varData.length} data points (SQL Warehouse)</div>
          </div>
          <ResponsiveContainer width="100%" height={350}>
            <LineChart data={varData}>
              <CartesianGrid strokeDasharray="3 3" stroke="#2a2a4a" />
              <XAxis dataKey="date" stroke="#606080" tick={{fontSize:11}} />
              <YAxis stroke="#606080" tick={{fontSize:11}} />
              <Tooltip contentStyle={{background:'#1a1a2e',border:'1px solid #2a2a4a',borderRadius:8}} labelStyle={{color:'#e8e8f0'}} />
              <Legend />
              <Line type="monotone" dataKey="var_99" stroke="#ef4444" strokeWidth={2} dot={false} name="VaR99" />
              <Line type="monotone" dataKey="es_99" stroke="#f97316" strokeWidth={1.5} dot={false} name="Expected Shortfall" strokeDasharray="5 5" />
            </LineChart>
          </ResponsiveContainer>
        </div>
      )}

      {/* VaR by Country */}
      {countryData && (
        <div className="card">
          <div className="card-header">
            <div className="card-title">VaR99 by Country</div>
            <div className="card-subtitle">Risk exposure by operating country</div>
          </div>
          <ResponsiveContainer width="100%" height={350}>
            <LineChart data={countryData}>
              <CartesianGrid strokeDasharray="3 3" stroke="#2a2a4a" />
              <XAxis dataKey="date" stroke="#606080" tick={{fontSize:11}} />
              <YAxis stroke="#606080" tick={{fontSize:11}} />
              <Tooltip contentStyle={{background:'#1a1a2e',border:'1px solid #2a2a4a',borderRadius:8}} labelStyle={{color:'#e8e8f0'}} />
              <Legend />
              {Object.entries(COUNTRY_COLORS).map(([c,col]) =>
                <Line key={c} type="monotone" dataKey={c} stroke={col} strokeWidth={2} dot={false} name={c} />
              )}
            </LineChart>
          </ResponsiveContainer>
        </div>
      )}

      {!varData && !loading && (
        <div className="card" style={{textAlign:'center',padding:40,color:'var(--text-muted)'}}>
          No aggregation data yet. Run Monte Carlo simulation and click "Persist to Delta" first.
        </div>
      )}
    </div>
  )
}
