import { useState, useEffect } from 'react'
import {
  LineChart, Line, XAxis, YAxis, CartesianGrid, Tooltip,
  ResponsiveContainer, Legend, ReferenceLine,
} from 'recharts'

export default function Compliance() {
  const [backtestData, setBacktestData] = useState(null)
  const [breachInfo, setBreachInfo] = useState(null)
  const [jobs, setJobs] = useState(null)
  const [jobRuns, setJobRuns] = useState(null)
  const [selectedJobId, setSelectedJobId] = useState(null)
  const [loading, setLoading] = useState(true)

  // Genie
  const [genieSpaceId, setGenieSpaceId] = useState('')
  const [genieQuestion, setGenieQuestion] = useState('')
  const [genieMessages, setGenieMessages] = useState([])
  const [genieLoading, setGenieLoading] = useState(false)

  useEffect(() => {
    Promise.all([
      fetch('/api/compliance/backtest').then(r => r.json()),
      fetch('/api/compliance/breaches').then(r => r.json()),
      fetch('/api/jobs/list').then(r => r.json()),
    ]).then(([bt, br, jb]) => {
      if (bt.rows) {
        setBacktestData(bt.rows.map(r => ({
          date: r.date?.substring(0, 10),
          portfolio_return: parseFloat(r.portfolio_return),
        })))
      }
      setBreachInfo(br)
      if (Array.isArray(jb)) setJobs(jb)
      setLoading(false)
    }).catch(() => setLoading(false))
  }, [])

  const loadJobRuns = (jobId) => {
    setSelectedJobId(jobId)
    fetch(`/api/jobs/runs/${jobId}`)
      .then(r => r.json())
      .then(data => {
        if (Array.isArray(data)) setJobRuns(data)
      })
  }

  const askGenie = () => {
    if (!genieSpaceId || !genieQuestion) return
    setGenieLoading(true)
    setGenieMessages(prev => [...prev, { role: 'user', content: genieQuestion }])

    fetch('/api/genie/ask', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ space_id: genieSpaceId, question: genieQuestion }),
    })
      .then(r => r.json())
      .then(data => {
        if (data.conversation_id && data.message_id) {
          // Poll for result
          const poll = () => {
            fetch(`/api/genie/result/${genieSpaceId}/${data.conversation_id}/${data.message_id}`)
              .then(r => r.json())
              .then(result => {
                if (result.status === 'COMPLETED' || result.status === 'FAILED') {
                  const text = result.result?.text || result.result?.description || 'No response'
                  const query = result.result?.query
                  let content = text
                  if (query) content += `\n\nSQL:\n${query}`
                  setGenieMessages(prev => [...prev, { role: 'assistant', content }])
                  setGenieLoading(false)
                } else {
                  setTimeout(poll, 2000)
                }
              })
          }
          setTimeout(poll, 3000)
        } else {
          setGenieMessages(prev => [...prev, { role: 'assistant', content: `Error: ${JSON.stringify(data)}` }])
          setGenieLoading(false)
        }
      })
      .catch(err => {
        setGenieMessages(prev => [...prev, { role: 'assistant', content: `Error: ${err.message}` }])
        setGenieLoading(false)
      })

    setGenieQuestion('')
  }

  if (loading) return <div className="loading"><div className="spinner" /> Loading...</div>

  return (
    <div>
      <div className="page-header">
        <h1>05 - Compliance</h1>
        <p>Basel backtesting, regulatory reporting, and interactive analysis</p>
      </div>

      {/* Basel Zones */}
      <div className="card">
        <div className="card-header">
          <div className="card-title">Basel Traffic Light Zones</div>
          <div className="card-subtitle">VaR backtesting classification</div>
        </div>
        <div className="grid-3">
          {breachInfo?.zones?.map(z => (
            <div key={z.level} style={{
              background: 'var(--bg-input)',
              border: `1px solid ${z.color}40`,
              borderRadius: 8,
              padding: 20,
              textAlign: 'center',
            }}>
              <div style={{ fontSize: 28, fontWeight: 700, color: z.color, marginBottom: 8 }}>
                {z.level}
              </div>
              <div style={{ fontSize: 13, color: 'var(--text-secondary)', marginBottom: 4 }}>
                {z.threshold}
              </div>
              <div style={{ fontSize: 12, color: 'var(--text-muted)' }}>
                {z.result}
              </div>
            </div>
          ))}
        </div>
      </div>

      {/* Backtest Chart */}
      {backtestData && (
        <div className="card">
          <div className="card-header">
            <div className="card-title">Portfolio Daily Returns</div>
            <div className="card-subtitle">{backtestData.length} trading days</div>
          </div>
          <ResponsiveContainer width="100%" height={350}>
            <LineChart data={backtestData}>
              <CartesianGrid strokeDasharray="3 3" stroke="#2a2a4a" />
              <XAxis dataKey="date" stroke="#606080" tick={{ fontSize: 11 }} />
              <YAxis stroke="#606080" tick={{ fontSize: 11 }} />
              <Tooltip
                contentStyle={{ background: '#1a1a2e', border: '1px solid #2a2a4a', borderRadius: 8 }}
                labelStyle={{ color: '#e8e8f0' }}
                formatter={(v) => [v.toFixed(6), 'Return']}
              />
              <ReferenceLine y={0} stroke="#606080" strokeDasharray="3 3" />
              <Line type="monotone" dataKey="portfolio_return" stroke="#22c55e" strokeWidth={1.5} dot={false} name="Portfolio Return" />
            </LineChart>
          </ResponsiveContainer>
        </div>
      )}

      {/* Genie Interactive */}
      <div className="card">
        <div className="card-header">
          <div className="card-title">Genie - Interactive Analysis</div>
          <div style={{ display: 'flex', gap: 8, alignItems: 'center' }}>
            <input
              className="form-input"
              style={{ width: 300 }}
              placeholder="Genie Space ID"
              value={genieSpaceId}
              onChange={e => setGenieSpaceId(e.target.value)}
            />
          </div>
        </div>
        <div className="genie-chat">
          <div className="genie-messages">
            {genieMessages.length === 0 && (
              <div style={{ textAlign: 'center', color: 'var(--text-muted)', padding: 40, fontSize: 14 }}>
                Enter your Genie Space ID above and ask questions about your VaR data.<br/>
                <div style={{ marginTop: 12, display: 'flex', gap: 8, justifyContent: 'center', flexWrap: 'wrap' }}>
                  {[
                    'What is the worst VaR day?',
                    'Show breach count by month',
                    'Which country has highest risk?',
                  ].map(q => (
                    <button
                      key={q}
                      className="btn btn-outline btn-sm"
                      onClick={() => setGenieQuestion(q)}
                    >
                      {q}
                    </button>
                  ))}
                </div>
              </div>
            )}
            {genieMessages.map((msg, i) => (
              <div key={i} className={`genie-message ${msg.role}`}>
                <pre style={{ whiteSpace: 'pre-wrap', margin: 0, fontFamily: 'inherit', fontSize: 'inherit' }}>
                  {msg.content}
                </pre>
              </div>
            ))}
            {genieLoading && (
              <div className="genie-message assistant">
                <div className="spinner" style={{ width: 16, height: 16, borderWidth: 2, display: 'inline-block', marginRight: 8 }} />
                Thinking...
              </div>
            )}
          </div>
          <div className="genie-input-row">
            <input
              className="form-input"
              placeholder={genieSpaceId ? 'Ask a question...' : 'Enter Genie Space ID first'}
              value={genieQuestion}
              onChange={e => setGenieQuestion(e.target.value)}
              onKeyDown={e => e.key === 'Enter' && askGenie()}
              disabled={!genieSpaceId}
            />
            <button className="btn btn-primary" onClick={askGenie} disabled={!genieSpaceId || !genieQuestion || genieLoading}>
              Send
            </button>
          </div>
        </div>
      </div>

      {/* Jobs & Audit Trail */}
      <div className="card">
        <div className="card-header">
          <div className="card-title">Jobs - Execution Audit Trail</div>
          <div className="card-subtitle">Databricks Jobs API</div>
        </div>
        {Array.isArray(jobs) && jobs.length > 0 ? (
          <>
            <div style={{ display: 'flex', gap: 8, marginBottom: 16, flexWrap: 'wrap' }}>
              {jobs.map(j => (
                <button
                  key={j.job_id}
                  className={`btn ${selectedJobId === j.job_id ? 'btn-primary' : 'btn-outline'} btn-sm`}
                  onClick={() => loadJobRuns(j.job_id)}
                >
                  {j.name || `Job ${j.job_id}`}
                </button>
              ))}
            </div>
            {jobRuns && Array.isArray(jobRuns) && (
              <table className="data-table">
                <thead>
                  <tr>
                    <th>Run ID</th>
                    <th>State</th>
                    <th>Result</th>
                    <th>Start Time</th>
                    <th>End Time</th>
                    <th>Link</th>
                  </tr>
                </thead>
                <tbody>
                  {jobRuns.map(r => (
                    <tr key={r.run_id}>
                      <td style={{ fontFamily: 'monospace' }}>{r.run_id}</td>
                      <td>
                        <span className={`badge ${r.state === 'TERMINATED' ? 'badge-green' : r.state === 'RUNNING' ? 'badge-blue' : 'badge-yellow'}`}>
                          {r.state}
                        </span>
                      </td>
                      <td>
                        <span className={`badge ${r.result_state === 'SUCCESS' ? 'badge-green' : r.result_state === 'FAILED' ? 'badge-red' : 'badge-yellow'}`}>
                          {r.result_state || '-'}
                        </span>
                      </td>
                      <td>{r.start_time || '-'}</td>
                      <td>{r.end_time || '-'}</td>
                      <td>
                        {r.run_page_url && (
                          <a href={r.run_page_url} target="_blank" rel="noopener noreferrer" className="btn btn-outline btn-sm">
                            Open
                          </a>
                        )}
                      </td>
                    </tr>
                  ))}
                </tbody>
              </table>
            )}
          </>
        ) : (
          <div style={{ color: 'var(--text-muted)', fontSize: 14, padding: 20, textAlign: 'center' }}>
            No VaR jobs found. Jobs will appear here after running the pipeline via Databricks Jobs.
          </div>
        )}
      </div>
    </div>
  )
}
