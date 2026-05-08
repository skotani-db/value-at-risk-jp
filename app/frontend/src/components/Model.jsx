import { useState, useEffect } from 'react'

export default function Model() {
  const [modelInfo, setModelInfo] = useState(null)
  const [versions, setVersions] = useState(null)
  const [accuracy, setAccuracy] = useState(null)
  const [loading, setLoading] = useState(true)

  useEffect(() => {
    Promise.all([
      fetch('/api/model/info').then(r => r.json()),
      fetch('/api/model/versions').then(r => r.json()),
      fetch('/api/model/accuracy').then(r => r.json()),
    ]).then(([info, vers, acc]) => {
      setModelInfo(info)
      setVersions(vers)
      setAccuracy(acc)
      setLoading(false)
    }).catch(() => setLoading(false))
  }, [])

  if (loading) return <div className="loading"><div className="spinner" /> Loading...</div>

  return (
    <div>
      <div className="page-header">
        <h1>02 - Model</h1>
        <p>Unity Catalog registered model metadata and version management</p>
      </div>

      {/* Model Overview */}
      <div className="grid-2">
        <div className="card">
          <div className="card-header">
            <div className="card-title">Model Overview</div>
          </div>
          {modelInfo && (
            <div style={{ display: 'flex', flexDirection: 'column', gap: 16 }}>
              <div>
                <div className="form-label">Model Name</div>
                <div style={{ fontSize: 16, fontWeight: 600, color: 'var(--accent-purple)' }}>
                  {modelInfo.name}
                </div>
              </div>
              <div>
                <div className="form-label">Description</div>
                <div style={{ fontSize: 14, color: 'var(--text-secondary)' }}>
                  {modelInfo.description || 'Non-linear features + OLS regression model for stock return prediction'}
                </div>
              </div>
              <div>
                <div className="form-label">Owner</div>
                <div style={{ fontSize: 14 }}>{modelInfo.owner || '-'}</div>
              </div>
              {modelInfo.created_at && (
                <div>
                  <div className="form-label">Created</div>
                  <div style={{ fontSize: 14 }}>{modelInfo.created_at}</div>
                </div>
              )}
              {modelInfo.error && (
                <div style={{ color: 'var(--accent-yellow)', fontSize: 13 }}>
                  Note: {modelInfo.error}
                </div>
              )}
            </div>
          )}
        </div>

        <div className="card">
          <div className="card-header">
            <div className="card-title">Model Architecture</div>
          </div>
          <div style={{ display: 'flex', flexDirection: 'column', gap: 16 }}>
            <div>
              <div className="form-label">Algorithm</div>
              <div style={{ fontSize: 14 }}>OLS Regression with Non-linear Features</div>
            </div>
            <div>
              <div className="form-label">Feature Engineering</div>
              <div style={{ fontSize: 13, color: 'var(--text-secondary)', lineHeight: 1.8 }}>
                For each market factor x:<br/>
                <code style={{ background: 'var(--bg-input)', padding: '2px 6px', borderRadius: 4, fontSize: 12 }}>
                  [x, x^2, x^3, sqrt(|x|)]
                </code>
                <br/>
                5 indicators x 4 features = 20 features + intercept
              </div>
            </div>
            <div>
              <div className="form-label">Market Factors</div>
              <div style={{ display: 'flex', gap: 6, flexWrap: 'wrap' }}>
                {['S&P500', 'NYSE', 'OIL', 'TREASURY', 'DOWJONES'].map(f => (
                  <span key={f} className="badge badge-purple">{f}</span>
                ))}
              </div>
            </div>
            <div>
              <div className="form-label">MLflow Tracking</div>
              <div style={{ fontSize: 13, color: 'var(--text-secondary)' }}>
                pyfunc model, logged with signature, correlation matrix artifact, WSSE metric
              </div>
            </div>
          </div>
        </div>
      </div>

      {/* Model Versions */}
      <div className="card">
        <div className="card-header">
          <div className="card-title">Model Versions</div>
          <div className="card-subtitle">Unity Catalog Registry</div>
        </div>
        {Array.isArray(versions) && versions.length > 0 ? (
          <table className="data-table">
            <thead>
              <tr>
                <th>Version</th>
                <th>Status</th>
                <th>Aliases</th>
                <th>Created</th>
                <th>Run ID</th>
              </tr>
            </thead>
            <tbody>
              {versions.map(v => (
                <tr key={v.version}>
                  <td style={{ fontWeight: 600 }}>v{v.version}</td>
                  <td>
                    <span className={`badge ${v.status === 'READY' ? 'badge-green' : 'badge-yellow'}`}>
                      {v.status || 'UNKNOWN'}
                    </span>
                  </td>
                  <td>
                    {v.aliases?.map(a => (
                      <span key={a} className="badge badge-blue" style={{ marginRight: 4 }}>{a}</span>
                    ))}
                  </td>
                  <td>{v.created_at || '-'}</td>
                  <td style={{ fontFamily: 'monospace', fontSize: 11 }}>{v.run_id?.substring(0, 12) || '-'}</td>
                </tr>
              ))}
            </tbody>
          </table>
        ) : (
          <div style={{ color: 'var(--text-muted)', fontSize: 14, padding: 20, textAlign: 'center' }}>
            {versions?.error ? `Error: ${versions.error}` : 'No model versions found. Run the pipeline first.'}
          </div>
        )}
      </div>

      {/* Per-ticker data */}
      <div className="card">
        <div className="card-header">
          <div className="card-title">Training Data by Ticker</div>
          <div className="card-subtitle">Data points used for model training</div>
        </div>
        {accuracy?.rows && (
          <div style={{ display: 'flex', flexWrap: 'wrap', gap: 8 }}>
            {accuracy.rows.map(r => (
              <div key={r.ticker} style={{
                background: 'var(--bg-input)',
                border: '1px solid var(--border-color)',
                borderRadius: 8,
                padding: '10px 14px',
                minWidth: 100,
                textAlign: 'center',
              }}>
                <div style={{ fontSize: 13, fontWeight: 600, color: 'var(--accent-purple)' }}>{r.ticker}</div>
                <div style={{ fontSize: 18, fontWeight: 700, marginTop: 4 }}>{parseInt(r.data_points).toLocaleString()}</div>
                <div style={{ fontSize: 11, color: 'var(--text-muted)' }}>data points</div>
              </div>
            ))}
          </div>
        )}
      </div>
    </div>
  )
}
