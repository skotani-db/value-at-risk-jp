import { useState, useEffect, useRef } from 'react'
import { BarChart, Bar, XAxis, YAxis, CartesianGrid, Tooltip, ResponsiveContainer, Legend, ScatterChart, Scatter, ZAxis, Cell } from 'recharts'

const FACTORS = ['SP500', 'NYSE', 'OIL', 'TREASURY', 'DOWJONES']
const FACTOR_COLORS = { beta_sp500:'#4f8cff', beta_nyse:'#22c55e', beta_oil:'#f97316', beta_treasury:'#a855f7', beta_dowjones:'#06b6d4' }

export default function Model() {
  const [training, setTraining] = useState(false)
  const [progress, setProgress] = useState(null)
  const [weights, setWeights] = useState(null)
  const [factorCorr, setFactorCorr] = useState(null)
  const [urls, setUrls] = useState(null)
  const [config, setConfig] = useState(null)
  const [trained, setTrained] = useState(false)
  const pollRef = useRef(null)

  useEffect(() => {
    fetch('/api/config').then(r=>r.json()).then(setConfig).catch(()=>{})
    fetch('/api/model/urls').then(r=>r.json()).then(setUrls).catch(()=>{})
    // Load existing results if model was previously trained
    loadResults()
  }, [])

  const loadResults = () => {
    fetch('/api/model/weights').then(r=>r.json()).then(d => {
      if (d.rows && d.rows.length > 0) { setWeights(d); setTrained(true) }
    }).catch(()=>{})
    fetch('/api/model/factor_corr').then(r=>r.json()).then(d => {
      if (d.rows && d.rows.length > 0) setFactorCorr(d.rows[0])
    }).catch(()=>{})
    fetch('/api/model/urls').then(r=>r.json()).then(setUrls).catch(()=>{})
  }

  const pollProgress = () => {
    pollRef.current = setInterval(async () => {
      try {
        const p = await fetch('/api/model/progress').then(r=>r.json())
        setProgress(p)
        if (!p.running) {
          clearInterval(pollRef.current); pollRef.current = null
          setTraining(false)
          if (p.done) loadResults()
        }
      } catch(e) {}
    }, 1500)
  }

  useEffect(() => () => { if (pollRef.current) clearInterval(pollRef.current) }, [])

  const trainModel = async () => {
    setTraining(true); setProgress(null)
    try {
      await fetch('/api/model/train', { method:'POST' })
      pollProgress()
    } catch(e) { setTraining(false) }
  }

  const pct = progress && progress.total > 0 ? Math.round(progress.current / progress.total * 100) : 0

  // Prepare chart data
  const betaChartData = weights?.rows?.map(r => ({
    ticker: r.ticker,
    SP500: parseFloat(r.beta_sp500||0),
    OIL: parseFloat(r.beta_oil||0),
    TREASURY: parseFloat(r.beta_treasury||0),
  })) || []

  const scatterData = weights?.rows?.map(r => ({
    ticker: r.ticker,
    volatility: parseFloat(r.volatility||0) * 100,
    corr_sp500: parseFloat(r.corr_sp500||0),
    beta_sp500: Math.abs(parseFloat(r.beta_sp500||0)),
  })) || []

  // Build correlation heatmap data
  const corrMatrix = factorCorr ? buildCorrMatrix(factorCorr) : null

  return (
    <div>
      <div className="page-header">
        <h1>02 - Model</h1>
        <p>Multi-factor risk model training and registration</p>
      </div>

      <div className="grid-2">
        {/* Architecture */}
        <div className="card">
          <div className="card-title" style={{marginBottom:16}}>Model Architecture</div>
          <div style={{display:'flex',flexDirection:'column',gap:14}}>
            <div>
              <div className="form-label">Algorithm</div>
              <div style={{fontSize:14}}>Multi-factor OLS Regression (per ticker)</div>
            </div>
            <div>
              <div className="form-label">Feature Engineering</div>
              <div style={{fontSize:13,color:'var(--text-secondary)',lineHeight:1.8}}>
                <code style={{background:'var(--bg-input)',padding:'2px 6px',borderRadius:4,fontSize:12}}>
                  R_stock = alpha + beta_1*R_SP500 + beta_2*R_OIL + beta_3*R_TREASURY + ...
                </code>
              </div>
            </div>
            <div>
              <div className="form-label">Market Factors</div>
              <div style={{display:'flex',gap:6,flexWrap:'wrap'}}>
                {FACTORS.map(f => <span key={f} className="badge badge-purple">{f}</span>)}
              </div>
            </div>
            <div>
              <div className="form-label">Training Data Cutoff</div>
              <div style={{fontSize:14}}>{config?.model?.date || '-'}</div>
            </div>
          </div>
        </div>

        {/* Training & Links */}
        <div className="card">
          <div className="card-title" style={{marginBottom:16}}>Training & Tracking</div>
          <div style={{display:'flex',flexDirection:'column',gap:14}}>
            <button className="btn btn-primary" onClick={trainModel} disabled={training}
              style={{width:'100%',justifyContent:'center'}}>
              {training ? 'Training...' : weights ? 'Re-train Model' : 'Train Model'}
            </button>

            {/* Progress */}
            {training && progress && (
              <div>
                <div style={{display:'flex',justifyContent:'space-between',marginBottom:4}}>
                  <span style={{fontSize:12,color:'var(--text-secondary)'}}>{progress.step}</span>
                  <span style={{fontSize:12,color:'var(--text-muted)'}}>{progress.current}/{progress.total}</span>
                </div>
                <div style={{width:'100%',height:6,background:'var(--bg-input)',borderRadius:3,overflow:'hidden'}}>
                  <div style={{width:`${pct}%`,height:'100%',background:'linear-gradient(90deg,var(--accent-purple),var(--accent-blue))',borderRadius:3,transition:'width 0.5s ease'}} />
                </div>
                {progress.run_url && (
                  <a href={progress.run_url} target="_blank" rel="noreferrer" style={{fontSize:12,color:'var(--accent-blue)',marginTop:6,display:'inline-block'}}>
                    View Job Run
                  </a>
                )}
              </div>
            )}
            {progress?.done && !training && (
              <div style={{fontSize:13,color:'var(--accent-green)'}}>
                Model trained & registered in Unity Catalog
              </div>
            )}
            {progress?.mlflow_error && !training && (
              <div style={{fontSize:12,color:'var(--accent-yellow)',marginTop:4}}>
                MLflow note: {progress.mlflow_error}
              </div>
            )}
            {progress?.error && (
              <div style={{fontSize:13,color:'var(--accent-red)'}}>Error: {progress.error}</div>
            )}

            {urls && (
              <>
                {[
                  { label:'MLflow Experiment', url: urls.experiment_url },
                  { label:'Unity Catalog Model Registry', url: urls.model_registry_url },
                  { label:'Data Catalog', url: urls.catalog_url },
                ].map(l => (
                  <div key={l.label}>
                    <div className="form-label">{l.label}</div>
                    {l.url ? (
                      <a href={l.url} target="_blank" rel="noreferrer" className="btn btn-outline btn-sm" style={{width:'100%',justifyContent:'center'}}>
                        Open {l.label}
                      </a>
                    ) : <div style={{fontSize:13,color:'var(--text-muted)'}}>URL not available</div>}
                  </div>
                ))}
              </>
            )}
          </div>
        </div>
      </div>

      {/* Factor Correlation Heatmap */}
      {trained && corrMatrix && (
        <div className="card">
          <div className="card-header">
            <div className="card-title">Factor Correlation Matrix</div>
            <div className="card-subtitle">Spearman correlation between market factors</div>
          </div>
          <div style={{display:'flex',justifyContent:'center',padding:16}}>
            <div>
              <div style={{display:'grid',gridTemplateColumns:`80px repeat(${FACTORS.length}, 72px)`,gap:2}}>
                <div />
                {FACTORS.map(f => <div key={f} style={{textAlign:'center',fontSize:10,fontWeight:600,color:'var(--text-muted)',padding:4}}>{f}</div>)}
                {FACTORS.map((row, ri) => (
                  <>
                    <div key={`label-${row}`} style={{fontSize:10,fontWeight:600,color:'var(--text-muted)',display:'flex',alignItems:'center',justifyContent:'flex-end',paddingRight:8}}>{row}</div>
                    {FACTORS.map((col, ci) => {
                      const val = ri === ci ? 1.0 : corrMatrix[`${Math.min(ri,ci)}_${Math.max(ri,ci)}`] || 0
                      const abs = Math.abs(val)
                      const bg = val > 0
                        ? `rgba(79,140,255,${abs * 0.7})`
                        : `rgba(239,68,68,${abs * 0.7})`
                      return (
                        <div key={`${row}-${col}`} style={{
                          width:72,height:40,display:'flex',alignItems:'center',justifyContent:'center',
                          background: ri===ci ? 'var(--bg-card-hover)' : bg,
                          borderRadius:4,fontSize:12,fontWeight:600,
                          color: abs > 0.3 || ri===ci ? 'var(--text-primary)' : 'var(--text-muted)',
                        }}>
                          {val.toFixed(2)}
                        </div>
                      )
                    })}
                  </>
                ))}
              </div>
            </div>
          </div>
        </div>
      )}

      {/* Beta Coefficients Bar Chart */}
      {trained && betaChartData.length > 0 && (
        <div className="card">
          <div className="card-header">
            <div className="card-title">Factor Sensitivity (Beta Coefficients)</div>
            <div className="card-subtitle">SP500, OIL, TREASURY betas per ticker</div>
          </div>
          <ResponsiveContainer width="100%" height={350}>
            <BarChart data={betaChartData} layout="vertical">
              <CartesianGrid strokeDasharray="3 3" stroke="#2a2a4a" />
              <XAxis type="number" stroke="#606080" tick={{fontSize:11}} />
              <YAxis type="category" dataKey="ticker" stroke="#606080" tick={{fontSize:11}} width={50} />
              <Tooltip contentStyle={{background:'#1a1a2e',border:'1px solid #2a2a4a',borderRadius:8}} />
              <Legend />
              <Bar dataKey="SP500" fill="#4f8cff" name="Beta SP500" barSize={6} />
              <Bar dataKey="OIL" fill="#f97316" name="Beta OIL" barSize={6} />
              <Bar dataKey="TREASURY" fill="#a855f7" name="Beta Treasury" barSize={6} />
            </BarChart>
          </ResponsiveContainer>
        </div>
      )}

      {/* Risk-Return Scatter */}
      {trained && scatterData.length > 0 && (
        <div className="card">
          <div className="card-header">
            <div className="card-title">Risk-Return Profile</div>
            <div className="card-subtitle">Volatility vs SP500 Correlation (bubble = |beta|)</div>
          </div>
          <ResponsiveContainer width="100%" height={350}>
            <ScatterChart>
              <CartesianGrid strokeDasharray="3 3" stroke="#2a2a4a" />
              <XAxis dataKey="volatility" name="Volatility (%)" stroke="#606080" tick={{fontSize:11}} label={{value:'Volatility (%)',position:'bottom',fill:'#606080',fontSize:12}} />
              <YAxis dataKey="corr_sp500" name="Corr(SP500)" stroke="#606080" tick={{fontSize:11}} label={{value:'Corr(SP500)',angle:-90,position:'insideLeft',fill:'#606080',fontSize:12}} />
              <ZAxis dataKey="beta_sp500" range={[40,400]} />
              <Tooltip contentStyle={{background:'#1a1a2e',border:'1px solid #2a2a4a',borderRadius:8}}
                formatter={(v,name) => [typeof v === 'number' ? v.toFixed(3) : v, name]}
                labelFormatter={() => ''}
                content={({payload}) => {
                  if (!payload?.[0]) return null
                  const d = payload[0].payload
                  return (
                    <div style={{background:'#1a1a2e',border:'1px solid #2a2a4a',borderRadius:8,padding:'8px 12px',fontSize:12}}>
                      <div style={{fontWeight:700,color:'var(--accent-blue)',marginBottom:4}}>{d.ticker}</div>
                      <div>Vol: {d.volatility.toFixed(2)}%</div>
                      <div>Corr: {d.corr_sp500.toFixed(3)}</div>
                    </div>
                  )
                }}
              />
              <Scatter data={scatterData}>
                {scatterData.map((d, i) => (
                  <Cell key={i} fill={d.corr_sp500 > 0 ? '#4f8cff' : '#ef4444'} fillOpacity={0.7} />
                ))}
              </Scatter>
            </ScatterChart>
          </ResponsiveContainer>
        </div>
      )}

      {/* Model Weights Table */}
      {trained && weights?.rows && (
        <div className="card">
          <div className="card-header">
            <div className="card-title">Model Coefficients</div>
            <div className="card-subtitle">{weights.rows.length} tickers</div>
          </div>
          <div style={{overflowX:'auto'}}>
            <table className="data-table">
              <thead><tr>
                <th>Ticker</th><th>Alpha</th><th>Beta SP500</th><th>Beta OIL</th><th>Beta Treasury</th>
                <th>Corr SP500</th><th>Volatility</th><th>N</th><th>Period</th>
              </tr></thead>
              <tbody>
                {weights.rows.map(r => (
                  <tr key={r.ticker}>
                    <td><span className="badge badge-purple">{r.ticker}</span></td>
                    <td>{parseFloat(r.alpha||0).toFixed(6)}</td>
                    <td style={{color:parseFloat(r.beta_sp500||0)>0?'var(--accent-green)':'var(--accent-red)'}}>{parseFloat(r.beta_sp500||0).toFixed(4)}</td>
                    <td>{parseFloat(r.beta_oil||0).toFixed(4)}</td>
                    <td>{parseFloat(r.beta_treasury||0).toFixed(4)}</td>
                    <td>{parseFloat(r.corr_sp500||0).toFixed(3)}</td>
                    <td>{(parseFloat(r.volatility||0)*100).toFixed(2)}%</td>
                    <td>{r.n_observations}</td>
                    <td style={{fontSize:11}}>{r.train_start?.substring(0,10)} ~ {r.train_end?.substring(0,10)}</td>
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

function buildCorrMatrix(raw) {
  // Map factor pairs to indices
  const pairs = {}
  const keys = [
    ['sp500','nyse',0,1], ['sp500','oil',0,2], ['sp500','treasury',0,3], ['sp500','dowjones',0,4],
    ['nyse','oil',1,2], ['nyse','treasury',1,3], ['nyse','dowjones',1,4],
    ['oil','treasury',2,3], ['oil','dowjones',2,4],
    ['treasury','dowjones',3,4],
  ]
  keys.forEach(([a,b,i,j]) => {
    const v = parseFloat(raw[`${a}_${b}`] || 0)
    pairs[`${i}_${j}`] = v
    pairs[`${j}_${i}`] = v
  })
  return pairs
}
