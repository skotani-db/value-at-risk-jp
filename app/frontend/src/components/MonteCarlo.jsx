import { useState, useEffect, useRef } from 'react'
import { BarChart, Bar, LineChart, Line, XAxis, YAxis, CartesianGrid, Tooltip, ResponsiveContainer, Legend, ReferenceLine } from 'recharts'

const COUNTRY_COLORS = {
  CHILE:'#06b6d4', COLOMBIA:'#a855f7', MEXICO:'#f97316', PANAMA:'#22c55e', PERU:'#ef4444',
}

export default function MonteCarlo() {
  const [params, setParams] = useState(null)
  const [numTrials, setNumTrials] = useState(5000)
  const [running, setRunning] = useState(false)
  const [progress, setProgress] = useState(null)
  const [histogram, setHistogram] = useState(null)
  const [totalTrials, setTotalTrials] = useState(0)
  const [varData, setVarData] = useState(null)
  const [countryData, setCountryData] = useState(null)
  const pollRef = useRef(null)
  const checkpointRef = useRef(null)

  useEffect(() => {
    fetch('/api/montecarlo/params').then(r=>r.json()).then(setParams).catch(()=>{})
    loadResults()
  }, [])

  const loadResults = () => {
    fetch('/api/aggregation/var_timeseries').then(r=>r.json()).then(d => {
      if (d.rows?.length > 0) setVarData(d.rows.map(r => ({ date: r.date?.substring(0,10), var_99: parseFloat(r.var_99) })))
    }).catch(()=>{})
    fetch('/api/aggregation/var_by_country').then(r=>r.json()).then(d => {
      if (d.rows?.length > 0) {
        const m = {}
        d.rows.forEach(r => { const dt = r.date?.substring(0,10); if(!m[dt]) m[dt]={date:dt}; m[dt][r.country]=parseFloat(r.var_99) })
        setCountryData(Object.values(m))
      }
    }).catch(()=>{})
  }

  // Poll checkpoint for live histogram
  const pollCheckpoint = () => {
    checkpointRef.current = setInterval(async () => {
      try {
        const d = await fetch('/api/montecarlo/checkpoint').then(r=>r.json())
        if (d.rows?.length > 0) {
          setHistogram(d.rows.map(r => ({ bucket: parseFloat(r.bucket), frequency: parseInt(r.frequency) })))
          setTotalTrials(parseInt(d.rows[0].total_trials || 0))
        }
      } catch(e) {}
    }, 5000)
  }

  const stopCheckpointPoll = () => {
    if (checkpointRef.current) { clearInterval(checkpointRef.current); checkpointRef.current = null }
  }

  // Poll job progress
  const pollProgress = () => {
    pollRef.current = setInterval(async () => {
      try {
        const p = await fetch('/api/montecarlo/progress').then(r=>r.json())
        setProgress(p)
        if (!p.running) {
          clearInterval(pollRef.current); pollRef.current = null
          stopCheckpointPoll()
          setRunning(false)
          if (p.done) loadResults()
        }
      } catch(e) {}
    }, 3000)
  }

  useEffect(() => () => {
    if (pollRef.current) clearInterval(pollRef.current)
    stopCheckpointPoll()
  }, [])

  const runSimulation = async () => {
    setRunning(true); setProgress(null); setHistogram(null); setTotalTrials(0)
    setVarData(null); setCountryData(null)
    try {
      await fetch('/api/montecarlo/run', {
        method:'POST', headers:{'Content-Type':'application/json'},
        body: JSON.stringify({ num_trials: numTrials })
      })
      pollProgress()
      pollCheckpoint()
    } catch(e) { setRunning(false) }
  }

  // Progress based on trial count, not job steps
  const trialPct = numTrials > 0 ? Math.min(100, Math.round(totalTrials / numTrials * 100)) : 0

  // Compute VaR from histogram
  const computeVarFromHist = () => {
    if (!histogram || histogram.length === 0) return null
    const total = histogram.reduce((s,h) => s + h.frequency, 0)
    const target = total * 0.01
    let cumul = 0
    for (const h of histogram) {
      cumul += h.frequency
      if (cumul >= target) return h.bucket
    }
    return null
  }
  const liveVar = computeVarFromHist()

  return (
    <div>
      <div className="page-header">
        <h1>03 - Monte Carlo Simulation</h1>
        <p>Multi-variate distribution sampling with live checkpoint visualization</p>
      </div>

      <div className="grid-2">
        {/* Controls */}
        <div className="card">
          <div className="card-title" style={{marginBottom:16}}>Simulation Parameters</div>
          <div className="form-group">
            <label className="form-label">Number of Trials</label>
            <input type="number" className="form-input" value={numTrials}
              onChange={e => setNumTrials(parseInt(e.target.value) || 1000)} />
          </div>
          {params && (
            <div style={{display:'flex',flexDirection:'column',gap:6,marginBottom:16,fontSize:12,color:'var(--text-muted)'}}>
              <div>Volatility Window: <strong>{params.volatility_window} days</strong></div>
              <div>Period: <strong>{params.model_date} ~ {params.max_date}</strong></div>
              <div>Sampling: <strong>Multivariate Normal</strong></div>
            </div>
          )}
          <button className="btn btn-primary" onClick={runSimulation} disabled={running} style={{width:'100%',justifyContent:'center'}}>
            {running ? 'Running...' : 'Run Monte Carlo Simulation'}
          </button>
        </div>

        {/* Progress */}
        <div className="card">
          <div className="card-title" style={{marginBottom:16}}>Job Progress</div>
          {running && progress ? (
            <div>
              <div style={{display:'flex',justifyContent:'space-between',marginBottom:4}}>
                <span style={{fontSize:12,color:'var(--text-secondary)'}}>
                  {totalTrials > 0 ? `${totalTrials.toLocaleString()} / ${numTrials.toLocaleString()} trials` : progress.step}
                </span>
                <span style={{fontSize:12,color:'var(--text-muted)'}}>{trialPct}%</span>
              </div>
              <div style={{width:'100%',height:8,background:'var(--bg-input)',borderRadius:4,overflow:'hidden'}}>
                <div style={{width:`${trialPct}%`,height:'100%',background:'linear-gradient(90deg,var(--accent-orange),var(--accent-red))',borderRadius:4,transition:'width 0.8s ease'}} />
              </div>
              {progress.run_url && (
                <a href={progress.run_url} target="_blank" rel="noreferrer" style={{fontSize:12,color:'var(--accent-blue)',marginTop:8,display:'inline-block'}}>View Job Run</a>
              )}
            </div>
          ) : progress?.done ? (
            <div>
              <div style={{fontSize:13,color:'var(--accent-green)',marginBottom:8}}>Simulation complete</div>
              {progress.run_url && <a href={progress.run_url} target="_blank" rel="noreferrer" className="btn btn-outline btn-sm">View Job Run</a>}
            </div>
          ) : progress?.error ? (
            <div style={{fontSize:13,color:'var(--accent-red)'}}>{progress.error}</div>
          ) : (
            <div style={{color:'var(--text-muted)',fontSize:13,padding:16,textAlign:'center'}}>
              Click "Run Monte Carlo Simulation" to start.<br/>
              <span style={{fontSize:11}}>Live histogram updates as samples accumulate.</span>
            </div>
          )}
          <div style={{marginTop:12,padding:10,background:'var(--bg-input)',borderRadius:8,fontSize:11,color:'var(--text-secondary)',lineHeight:1.7}}>
            <strong>Pipeline:</strong> Volatility stats → Sample market conditions → Predict returns (MLflow UDF) → Vectorize → Aggregate VaR → Compliance backtest
          </div>
        </div>
      </div>

      {/* Live Histogram */}
      {histogram && (
        <div className="card">
          <div className="card-header">
            <div className="card-title">
              Portfolio Return Distribution
              {running && <span style={{color:'var(--accent-orange)',fontSize:13,marginLeft:8}}>Live</span>}
            </div>
            <div className="card-subtitle">
              {totalTrials.toLocaleString()} trials
              {liveVar !== null && <span style={{marginLeft:12,color:'var(--accent-red)'}}>VaR99 = {liveVar.toFixed(4)}</span>}
            </div>
          </div>
          <ResponsiveContainer width="100%" height={400}>
            <BarChart data={histogram}>
              <CartesianGrid strokeDasharray="3 3" stroke="#2a2a4a" />
              <XAxis dataKey="bucket" stroke="#606080" tick={{fontSize:10}} tickFormatter={v=>v.toFixed(3)} />
              <YAxis stroke="#606080" tick={{fontSize:11}} />
              <Tooltip
                contentStyle={{background:'#1a1a2e',border:'1px solid #2a2a4a',borderRadius:8}}
                labelFormatter={v => `Return: ${parseFloat(v).toFixed(4)}`}
                formatter={(v) => [v, 'Frequency']}
              />
              {liveVar !== null && (
                <ReferenceLine x={liveVar} stroke="#ef4444" strokeDasharray="5 5"
                  label={{value:`VaR99`,fill:'#ef4444',fontSize:12,position:'top'}} />
              )}
              <Bar dataKey="frequency" fill="#4f8cff" fillOpacity={0.8} />
            </BarChart>
          </ResponsiveContainer>
        </div>
      )}

      {/* VaR Timeseries (after completion) */}
      {varData && (
        <div className="card">
          <div className="card-header">
            <div className="card-title">Portfolio VaR99 Over Time</div>
            <div className="card-subtitle">{varData.length} data points</div>
          </div>
          <ResponsiveContainer width="100%" height={350}>
            <LineChart data={varData}>
              <CartesianGrid strokeDasharray="3 3" stroke="#2a2a4a" />
              <XAxis dataKey="date" stroke="#606080" tick={{fontSize:11}} />
              <YAxis stroke="#606080" tick={{fontSize:11}} />
              <Tooltip contentStyle={{background:'#1a1a2e',border:'1px solid #2a2a4a',borderRadius:8}} labelStyle={{color:'#e8e8f0'}} />
              <Line type="monotone" dataKey="var_99" stroke="#ef4444" strokeWidth={2} dot={false} name="VaR99" />
            </LineChart>
          </ResponsiveContainer>
        </div>
      )}

      {/* VaR by Country */}
      {countryData && (
        <div className="card">
          <div className="card-header">
            <div className="card-title">VaR99 by Country</div>
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
    </div>
  )
}
