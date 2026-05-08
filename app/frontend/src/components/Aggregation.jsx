import { useState, useEffect } from 'react'
import { LineChart, Line, BarChart, Bar, XAxis, YAxis, CartesianGrid, Tooltip, ResponsiveContainer, Legend, AreaChart, Area } from 'recharts'

const COUNTRY_COLORS = {
  CHILE:'#06b6d4', COLOMBIA:'#a855f7', MEXICO:'#f97316', PANAMA:'#22c55e', PERU:'#ef4444',
}

export default function Aggregation() {
  const [varData, setVarData] = useState(null)
  const [countryData, setCountryData] = useState(null)
  const [complianceData, setComplianceData] = useState(null)
  const [summaryStats, setSummaryStats] = useState(null)
  const [loading, setLoading] = useState(true)

  useEffect(() => {
    Promise.all([
      fetch('/api/aggregation/var_timeseries').then(r=>r.json()).catch(()=>({rows:[]})),
      fetch('/api/aggregation/var_by_country').then(r=>r.json()).catch(()=>({rows:[]})),
      fetch('/api/compliance/data').then(r=>r.json()).catch(()=>({rows:[]})),
      fetch('/api/compliance/summary').then(r=>r.json()).catch(()=>({rows:[]})),
    ]).then(([vd, cd, comp, summary]) => {
      if (vd.rows?.length > 0) {
        setVarData(vd.rows.map(r => ({
          date: r.date?.substring(0,10),
          var_99: parseFloat(r.var_99 || 0),
          num_trials: parseInt(r.num_trials || 0),
        })))
      }
      if (cd.rows?.length > 0) {
        const m = {}
        cd.rows.forEach(r => {
          const dt = r.date?.substring(0,10)
          if(!m[dt]) m[dt] = { date: dt }
          m[dt][r.country] = parseFloat(r.var_99 || 0)
        })
        setCountryData(Object.values(m))
      }
      if (comp.rows?.length > 0) {
        setComplianceData(comp.rows.map(r => ({
          date: r.date?.substring(0,10),
          portfolio_return: parseFloat(r.portfolio_return || 0),
          var_99: parseFloat(r.var_99 || 0),
          is_breach: parseInt(r.is_breach || 0),
        })))
      }
      if (summary.rows?.[0]) setSummaryStats(summary.rows[0])
      setLoading(false)
    })
  }, [])

  if (loading) return <div className="loading"><div className="spinner"/>Loading...</div>

  if (!varData) return (
    <div>
      <div className="page-header">
        <h1>04 - Aggregation</h1>
        <p>Portfolio VaR aggregation dashboard</p>
      </div>
      <div className="card" style={{textAlign:'center',padding:40,color:'var(--text-muted)'}}>
        No aggregation data yet. Run Monte Carlo simulation first.
      </div>
    </div>
  )

  // Compute summary from data
  const worstVar = varData ? Math.min(...varData.map(d=>d.var_99)) : 0
  const avgVar = varData ? varData.reduce((s,d)=>s+d.var_99,0)/varData.length : 0
  const latestVar = varData ? varData[varData.length-1]?.var_99 : 0
  const breachCount = complianceData ? complianceData.filter(d=>d.is_breach===1).length : 0
  const totalDays = complianceData ? complianceData.length : 0
  const baselZone = breachCount <= 4 ? 'GREEN' : breachCount <= 9 ? 'YELLOW' : 'RED'
  const baselColor = baselZone === 'GREEN' ? '#22c55e' : baselZone === 'YELLOW' ? '#eab308' : '#ef4444'

  // Return vs VaR overlay
  const overlayData = complianceData?.map(d => ({
    date: d.date,
    return: d.portfolio_return,
    var_99: d.var_99,
  })) || []

  return (
    <div>
      <div className="page-header">
        <h1>04 - Aggregation</h1>
        <p>Portfolio VaR aggregation dashboard</p>
      </div>

      {/* Summary Cards */}
      <div className="grid-4" style={{marginBottom:24}}>
        <div className="stat-card">
          <div className="stat-label">Latest VaR99</div>
          <div className="stat-value red" style={{fontSize:22}}>{latestVar.toFixed(4)}</div>
        </div>
        <div className="stat-card">
          <div className="stat-label">Worst VaR99</div>
          <div className="stat-value" style={{fontSize:22,color:'var(--accent-orange)'}}>{worstVar.toFixed(4)}</div>
        </div>
        <div className="stat-card">
          <div className="stat-label">Avg VaR99</div>
          <div className="stat-value blue" style={{fontSize:22}}>{avgVar.toFixed(4)}</div>
        </div>
        <div className="stat-card">
          <div className="stat-label">Basel Zone</div>
          <div className="stat-value" style={{fontSize:22,color:baselColor}}>{baselZone}</div>
          <div style={{fontSize:11,color:'var(--text-muted)'}}>{breachCount} breaches / {totalDays} days</div>
        </div>
      </div>

      {/* VaR Over Time */}
      <div className="card">
        <div className="card-header">
          <div className="card-title">Portfolio VaR99 Over Time</div>
          <div className="card-subtitle">{varData.length} data points</div>
        </div>
        <ResponsiveContainer width="100%" height={300}>
          <AreaChart data={varData}>
            <CartesianGrid strokeDasharray="3 3" stroke="#2a2a4a" />
            <XAxis dataKey="date" stroke="#606080" tick={{fontSize:11}} />
            <YAxis stroke="#606080" tick={{fontSize:11}} />
            <Tooltip contentStyle={{background:'#1a1a2e',border:'1px solid #2a2a4a',borderRadius:8}} labelStyle={{color:'#e8e8f0'}} />
            <Area type="monotone" dataKey="var_99" stroke="#ef4444" fill="rgba(239,68,68,0.15)" strokeWidth={2} name="VaR99" />
          </AreaChart>
        </ResponsiveContainer>
      </div>

      {/* Return vs VaR Overlay */}
      {overlayData.length > 0 && (
        <div className="card">
          <div className="card-header">
            <div className="card-title">Portfolio Return vs VaR99</div>
            <div className="card-subtitle">Breach detection</div>
          </div>
          <ResponsiveContainer width="100%" height={300}>
            <LineChart data={overlayData}>
              <CartesianGrid strokeDasharray="3 3" stroke="#2a2a4a" />
              <XAxis dataKey="date" stroke="#606080" tick={{fontSize:11}} />
              <YAxis stroke="#606080" tick={{fontSize:11}} />
              <Tooltip contentStyle={{background:'#1a1a2e',border:'1px solid #2a2a4a',borderRadius:8}} labelStyle={{color:'#e8e8f0'}} />
              <Legend />
              <Line type="monotone" dataKey="return" stroke="#22c55e" strokeWidth={1.5} dot={false} name="Portfolio Return" />
              <Line type="monotone" dataKey="var_99" stroke="#ef4444" strokeWidth={2} dot={false} name="VaR99" strokeDasharray="5 5" />
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
          <ResponsiveContainer width="100%" height={300}>
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
    </div>
  )
}
