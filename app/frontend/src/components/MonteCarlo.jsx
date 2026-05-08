import { useState, useEffect } from 'react'
import {
  LineChart, Line, XAxis, YAxis, CartesianGrid, Tooltip,
  ResponsiveContainer, BarChart, Bar, Legend,
} from 'recharts'

export default function MonteCarlo() {
  const [params, setParams] = useState(null)
  const [varData, setVarData] = useState(null)
  const [trialsSummary, setTrialsSummary] = useState(null)
  const [loading, setLoading] = useState(true)
  const [confidence, setConfidence] = useState(99)
  const [loadingVar, setLoadingVar] = useState(false)

  // Editable params for display
  const [editRuns, setEditRuns] = useState(32000)
  const [editVol, setEditVol] = useState(90)

  useEffect(() => {
    Promise.all([
      fetch('/api/montecarlo/params').then(r => r.json()),
      fetch('/api/montecarlo/trials/summary').then(r => r.json()),
    ]).then(([p, ts]) => {
      setParams(p)
      setEditRuns(p.runs)
      setEditVol(p.volatility_window)
      setTrialsSummary(ts)
      setLoading(false)
    }).catch(() => setLoading(false))
  }, [])

  const fetchVar = () => {
    setLoadingVar(true)
    fetch(`/api/montecarlo/var?confidence=${confidence}`)
      .then(r => r.json())
      .then(data => {
        if (data.rows) {
          setVarData(data.rows.map(r => ({
            date: r.date?.substring(0, 10),
            var_value: parseFloat(r.var_value),
            num_simulations: parseInt(r.num_simulations),
          })))
        }
        setLoadingVar(false)
      })
      .catch(() => setLoadingVar(false))
  }

  if (loading) return <div className="loading"><div className="spinner" /> Loading...</div>

  return (
    <div>
      <div className="page-header">
        <h1>03 - Monte Carlo Simulation</h1>
        <p>Parallel simulation engine using market volatility distributions</p>
      </div>

      {/* Parameters */}
      <div className="grid-2">
        <div className="card">
          <div className="card-header">
            <div className="card-title">Simulation Parameters</div>
          </div>
          <div style={{ display: 'flex', flexDirection: 'column', gap: 16 }}>
            <div className="form-group">
              <label className="form-label">Trials per Ticker</label>
              <input
                type="number"
                className="form-input"
                value={editRuns}
                onChange={e => setEditRuns(parseInt(e.target.value) || 0)}
              />
            </div>
            <div className="form-group">
              <label className="form-label">Volatility Window (days)</label>
              <input
                type="number"
                className="form-input"
                value={editVol}
                onChange={e => setEditVol(parseInt(e.target.value) || 0)}
              />
            </div>
            <div className="form-group">
              <label className="form-label">Confidence Level (%)</label>
              <select
                className="form-select"
                value={confidence}
                onChange={e => setConfidence(parseInt(e.target.value))}
              >
                <option value={95}>95%</option>
                <option value={97}>97.5%</option>
                <option value={99}>99%</option>
              </select>
            </div>
            <button className="btn btn-primary" onClick={fetchVar} disabled={loadingVar}>
              {loadingVar ? 'Calculating...' : 'Calculate VaR'}
            </button>
          </div>
        </div>

        <div className="card">
          <div className="card-header">
            <div className="card-title">Current Configuration</div>
          </div>
          {params && (
            <div style={{ display: 'flex', flexDirection: 'column', gap: 16 }}>
              <div className="grid-2">
                <div>
                  <div className="stat-label">Executors</div>
                  <div className="stat-value blue" style={{ fontSize: 24 }}>{params.executors}</div>
                </div>
                <div>
                  <div className="stat-label">Total Trials</div>
                  <div className="stat-value purple" style={{ fontSize: 24 }}>{params.runs.toLocaleString()}</div>
                </div>
              </div>
              <div>
                <div className="stat-label">Data Range</div>
                <div style={{ fontSize: 14, color: 'var(--text-secondary)' }}>
                  {params.data_range.min} ~ {params.data_range.max}
                </div>
              </div>
              <div>
                <div className="stat-label">Model Date</div>
                <div style={{ fontSize: 14, color: 'var(--text-secondary)' }}>
                  {params.model_date}
                </div>
              </div>
              <div>
                <div className="stat-label">Sampling Method</div>
                <div style={{ fontSize: 13, color: 'var(--text-secondary)' }}>
                  Multivariate Normal Distribution<br/>
                  (Cholesky decomposition for correlated factors)
                </div>
              </div>
            </div>
          )}
        </div>
      </div>

      {/* VaR Chart */}
      {varData && (
        <div className="card">
          <div className="card-header">
            <div className="card-title">VaR{confidence} Over Time</div>
            <div className="card-subtitle">{varData.length} time points</div>
          </div>
          <ResponsiveContainer width="100%" height={400}>
            <LineChart data={varData}>
              <CartesianGrid strokeDasharray="3 3" stroke="#2a2a4a" />
              <XAxis dataKey="date" stroke="#606080" tick={{ fontSize: 11 }} />
              <YAxis stroke="#606080" tick={{ fontSize: 11 }} />
              <Tooltip
                contentStyle={{ background: '#1a1a2e', border: '1px solid #2a2a4a', borderRadius: 8 }}
                labelStyle={{ color: '#e8e8f0' }}
                formatter={(v) => [v.toFixed(6), `VaR${confidence}`]}
              />
              <Line type="monotone" dataKey="var_value" stroke="#ef4444" strokeWidth={2} dot={false} name={`VaR${confidence}`} />
            </LineChart>
          </ResponsiveContainer>
        </div>
      )}

      {/* Trials Summary */}
      {trialsSummary?.rows && (
        <div className="card">
          <div className="card-header">
            <div className="card-title">Trial Distribution</div>
            <div className="card-subtitle">Simulations per ticker/date</div>
          </div>
          <div style={{ overflowX: 'auto', maxHeight: 400 }}>
            <table className="data-table">
              <thead>
                <tr>
                  <th>Date</th>
                  <th>Ticker</th>
                  <th>Num Trials</th>
                </tr>
              </thead>
              <tbody>
                {trialsSummary.rows.slice(0, 50).map((r, i) => (
                  <tr key={i}>
                    <td>{r.date?.substring(0, 10)}</td>
                    <td><span className="badge badge-blue">{r.ticker}</span></td>
                    <td>{parseInt(r.num_trials).toLocaleString()}</td>
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
