import { useState, useEffect } from 'react'
import {
  LineChart, Line, XAxis, YAxis, CartesianGrid, Tooltip,
  ResponsiveContainer, Legend,
} from 'recharts'

const COUNTRY_COLORS = {
  CHILE: '#06b6d4',
  COLOMBIA: '#a855f7',
  MEXICO: '#f97316',
  PANAMA: '#22c55e',
  PERU: '#ef4444',
}

export default function Aggregation() {
  const [varByDate, setVarByDate] = useState(null)
  const [varByCountry, setVarByCountry] = useState(null)
  const [dashboardUrl, setDashboardUrl] = useState('')
  const [loading, setLoading] = useState(true)

  useEffect(() => {
    Promise.all([
      fetch('/api/aggregation/var_by_date').then(r => r.json()),
      fetch('/api/aggregation/var_by_country').then(r => r.json()),
    ]).then(([byDate, byCountry]) => {
      if (byDate.rows) {
        setVarByDate(byDate.rows.map(r => ({
          date: r.date?.substring(0, 10),
          var_99: parseFloat(r.var_99),
        })))
      }
      if (byCountry.rows) {
        // Pivot country data
        const dateMap = {}
        byCountry.rows.forEach(r => {
          const d = r.date?.substring(0, 10)
          if (!dateMap[d]) dateMap[d] = { date: d }
          dateMap[d][r.country] = parseFloat(r.var_99)
        })
        setVarByCountry(Object.values(dateMap))
      }
      setLoading(false)
    }).catch(() => setLoading(false))
  }, [])

  if (loading) return <div className="loading"><div className="spinner" /> Loading...</div>

  return (
    <div>
      <div className="page-header">
        <h1>04 - Aggregation</h1>
        <p>Portfolio-level VaR aggregation with slice & dice analysis</p>
      </div>

      {/* Dashboard Embed */}
      <div className="card">
        <div className="card-header">
          <div className="card-title">AI/BI Dashboard</div>
          <div style={{ display: 'flex', gap: 8, alignItems: 'center' }}>
            <input
              className="form-input"
              style={{ width: 400 }}
              placeholder="Paste your AI/BI Dashboard embed URL here"
              value={dashboardUrl}
              onChange={e => setDashboardUrl(e.target.value)}
            />
          </div>
        </div>
        {dashboardUrl ? (
          <iframe
            src={dashboardUrl}
            className="dashboard-embed"
            title="AI/BI Dashboard"
            allow="fullscreen"
          />
        ) : (
          <div style={{
            padding: 60, textAlign: 'center',
            background: 'var(--bg-input)', borderRadius: 8,
            color: 'var(--text-muted)', fontSize: 14
          }}>
            Enter your AI/BI (Lakeview) Dashboard embed URL above to display it here.<br/>
            <span style={{ fontSize: 12 }}>
              Format: https://&lt;workspace&gt;.databricks.com/embed/dashboardsv3/...
            </span>
          </div>
        )}
      </div>

      {/* Portfolio VaR over time */}
      {varByDate && (
        <div className="card">
          <div className="card-header">
            <div className="card-title">Portfolio VaR99 Over Time</div>
            <div className="card-subtitle">{varByDate.length} data points</div>
          </div>
          <ResponsiveContainer width="100%" height={350}>
            <LineChart data={varByDate}>
              <CartesianGrid strokeDasharray="3 3" stroke="#2a2a4a" />
              <XAxis dataKey="date" stroke="#606080" tick={{ fontSize: 11 }} />
              <YAxis stroke="#606080" tick={{ fontSize: 11 }} />
              <Tooltip
                contentStyle={{ background: '#1a1a2e', border: '1px solid #2a2a4a', borderRadius: 8 }}
                labelStyle={{ color: '#e8e8f0' }}
                formatter={(v) => [v.toFixed(6), 'VaR99']}
              />
              <Line type="monotone" dataKey="var_99" stroke="#ef4444" strokeWidth={2} dot={false} name="VaR99" />
            </LineChart>
          </ResponsiveContainer>
        </div>
      )}

      {/* VaR by Country */}
      {varByCountry && varByCountry.length > 0 && (
        <div className="card">
          <div className="card-header">
            <div className="card-title">VaR99 by Country</div>
            <div className="card-subtitle">Risk exposure by operating country</div>
          </div>
          <ResponsiveContainer width="100%" height={350}>
            <LineChart data={varByCountry}>
              <CartesianGrid strokeDasharray="3 3" stroke="#2a2a4a" />
              <XAxis dataKey="date" stroke="#606080" tick={{ fontSize: 11 }} />
              <YAxis stroke="#606080" tick={{ fontSize: 11 }} />
              <Tooltip
                contentStyle={{ background: '#1a1a2e', border: '1px solid #2a2a4a', borderRadius: 8 }}
                labelStyle={{ color: '#e8e8f0' }}
              />
              <Legend />
              {Object.entries(COUNTRY_COLORS).map(([country, color]) => (
                <Line
                  key={country}
                  type="monotone"
                  dataKey={country}
                  stroke={color}
                  strokeWidth={2}
                  dot={false}
                  name={country}
                />
              ))}
            </LineChart>
          </ResponsiveContainer>
        </div>
      )}
    </div>
  )
}
