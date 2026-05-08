import { useState, useEffect, useRef } from 'react'
import { BarChart, Bar, XAxis, YAxis, CartesianGrid, Tooltip, ResponsiveContainer, ReferenceLine } from 'recharts'

const STEPS = [500, 2000, 8000, 32000]

export default function MonteCarlo() {
  const [confidence, setConfidence] = useState(99)
  const [running, setRunning] = useState(false)
  const [currentStep, setCurrentStep] = useState(-1)
  const [histogram, setHistogram] = useState(null)
  const [stats, setStats] = useState(null)
  const [stepHistory, setStepHistory] = useState([])
  const cancelRef = useRef(false)

  const runProgressive = async () => {
    setRunning(true)
    cancelRef.current = false
    setStepHistory([])
    setHistogram(null)
    setStats(null)

    for (let i = 0; i < STEPS.length; i++) {
      if (cancelRef.current) break
      setCurrentStep(i)
      try {
        const r = await fetch(`/api/montecarlo/step?num_trials=${STEPS[i]}&confidence=${confidence}`, { method: 'POST' })
        const data = await r.json()
        if (data.histogram?.rows) {
          setHistogram(data.histogram.rows.map(r => ({
            bucket: parseFloat(r.bucket),
            frequency: parseInt(r.frequency),
          })))
        }
        if (data.stats?.rows?.[0]) {
          const s = data.stats.rows[0]
          const stat = {
            trials: STEPS[i],
            var_value: parseFloat(s.var_value || 0),
            expected_shortfall: parseFloat(s.expected_shortfall || 0),
            mean_return: parseFloat(s.mean_return || 0),
            std_return: parseFloat(s.std_return || 0),
          }
          setStats(stat)
          setStepHistory(prev => [...prev, stat])
        }
      } catch(e) { console.error(e) }
    }
    setRunning(false)
    setCurrentStep(-1)
  }

  const persistResults = async () => {
    setRunning(true)
    try {
      await fetch('/api/montecarlo/persist', {
        method: 'POST',
        headers: {'Content-Type':'application/json'},
        body: JSON.stringify({ num_trials: 1000, confidence })
      })
      alert('Results persisted to Delta tables for Aggregation page')
    } catch(e) { alert('Error: ' + e.message) }
    setRunning(false)
  }

  return (
    <div>
      <div className="page-header">
        <h1>03 - Monte Carlo Simulation</h1>
        <p>Progressive simulation with real-time distribution refinement</p>
      </div>

      {/* Controls */}
      <div className="grid-2">
        <div className="card">
          <div className="card-title" style={{marginBottom:16}}>Simulation Parameters</div>
          <div className="form-group">
            <label className="form-label">Confidence Level</label>
            <select className="form-select" value={confidence} onChange={e=>setConfidence(parseInt(e.target.value))}>
              <option value={95}>95%</option>
              <option value={97}>97.5%</option>
              <option value={99}>99%</option>
            </select>
          </div>
          <div className="form-group">
            <label className="form-label">Progressive Steps</label>
            <div style={{fontSize:13,color:'var(--text-secondary)'}}>
              {STEPS.map((s,i) => (
                <span key={s} style={{
                  color: i === currentStep ? 'var(--accent-blue)' :
                         i < currentStep || (currentStep === -1 && stepHistory.length > i) ? 'var(--accent-green)' : 'var(--text-muted)',
                  fontWeight: i === currentStep ? 700 : 400,
                }}>
                  {s.toLocaleString()}{i < STEPS.length - 1 ? ' → ' : ' trials'}
                </span>
              ))}
            </div>
          </div>
          <div style={{display:'flex',gap:8}}>
            <button className="btn btn-primary" onClick={runProgressive} disabled={running}>
              {running ? `Running... (${STEPS[currentStep]?.toLocaleString() || ''})` : 'Run Simulation'}
            </button>
            {stepHistory.length > 0 && (
              <button className="btn btn-outline" onClick={persistResults} disabled={running}>
                Persist to Delta
              </button>
            )}
          </div>
        </div>

        {/* Live Stats */}
        <div className="card">
          <div className="card-title" style={{marginBottom:16}}>
            {stats ? `VaR${confidence} Results` : 'Waiting for simulation...'}
          </div>
          {stats ? (
            <div className="grid-2">
              <div>
                <div className="stat-label">VaR{confidence}</div>
                <div className="stat-value red" style={{fontSize:22}}>{stats.var_value.toFixed(6)}</div>
              </div>
              <div>
                <div className="stat-label">Expected Shortfall</div>
                <div className="stat-value" style={{fontSize:22,color:'var(--accent-orange)'}}>{stats.expected_shortfall.toFixed(6)}</div>
              </div>
              <div>
                <div className="stat-label">Mean Return</div>
                <div style={{fontSize:18,fontWeight:600}}>{stats.mean_return.toFixed(6)}</div>
              </div>
              <div>
                <div className="stat-label">Std Dev</div>
                <div style={{fontSize:18,fontWeight:600}}>{stats.std_return.toFixed(6)}</div>
              </div>
            </div>
          ) : (
            <div style={{color:'var(--text-muted)',fontSize:14,padding:20,textAlign:'center'}}>
              Click "Run Simulation" to start
            </div>
          )}
        </div>
      </div>

      {/* Histogram */}
      {histogram && (
        <div className="card">
          <div className="card-header">
            <div className="card-title">Return Distribution</div>
            <div className="card-subtitle">
              {stats?.trials ? `${stats.trials.toLocaleString()} trials` : ''}
              {running && <span style={{color:'var(--accent-blue)',marginLeft:8}}>Refining...</span>}
            </div>
          </div>
          <ResponsiveContainer width="100%" height={400}>
            <BarChart data={histogram}>
              <CartesianGrid strokeDasharray="3 3" stroke="#2a2a4a" />
              <XAxis dataKey="bucket" stroke="#606080" tick={{fontSize:10}} tickFormatter={v=>v.toFixed(3)} />
              <YAxis stroke="#606080" tick={{fontSize:11}} />
              <Tooltip
                contentStyle={{background:'#1a1a2e',border:'1px solid #2a2a4a',borderRadius:8}}
                labelStyle={{color:'#e8e8f0'}}
                formatter={(v,name) => [v, 'Frequency']}
                labelFormatter={v => `Return: ${parseFloat(v).toFixed(4)}`}
              />
              {stats && <ReferenceLine x={stats.var_value} stroke="#ef4444" strokeDasharray="5 5" label={{value:`VaR${confidence}`,fill:'#ef4444',fontSize:12}} />}
              <Bar dataKey="frequency" fill="#4f8cff" fillOpacity={0.8} />
            </BarChart>
          </ResponsiveContainer>
        </div>
      )}

      {/* Step convergence */}
      {stepHistory.length > 1 && (
        <div className="card">
          <div className="card-title" style={{marginBottom:16}}>Convergence</div>
          <table className="data-table">
            <thead><tr><th>Trials</th><th>VaR{confidence}</th><th>ES{confidence}</th><th>Mean</th><th>Std</th></tr></thead>
            <tbody>
              {stepHistory.map((s,i) => (
                <tr key={i}>
                  <td style={{fontWeight:600}}>{s.trials.toLocaleString()}</td>
                  <td style={{color:'var(--accent-red)'}}>{s.var_value.toFixed(6)}</td>
                  <td style={{color:'var(--accent-orange)'}}>{s.expected_shortfall.toFixed(6)}</td>
                  <td>{s.mean_return.toFixed(6)}</td>
                  <td>{s.std_return.toFixed(6)}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      )}
    </div>
  )
}
