import { useState, useEffect } from 'react'
import { LineChart, Line, XAxis, YAxis, CartesianGrid, Tooltip, ResponsiveContainer, Legend, ReferenceLine } from 'recharts'

export default function Compliance() {
  const [compData, setCompData] = useState(null)
  const [summary, setSummary] = useState(null)
  const [computing, setComputing] = useState(false)
  const [loading, setLoading] = useState(true)

  // Genie
  const [genieSpaceId, setGenieSpaceId] = useState('')
  const [genieQuestion, setGenieQuestion] = useState('')
  const [genieMessages, setGenieMessages] = useState([])
  const [genieLoading, setGenieLoading] = useState(false)

  const loadData = () => {
    Promise.all([
      fetch('/api/compliance/data').then(r => r.json()),
      fetch('/api/compliance/summary').then(r => r.json()),
    ]).then(([cd, sm]) => {
      if (cd.rows && cd.rows.length > 0) {
        setCompData(cd.rows.map(r => ({
          date: r.date?.substring(0,10),
          portfolio_return: parseFloat(r.portfolio_return || 0),
          var_99: parseFloat(r.var_99 || 0),
          is_breach: parseInt(r.is_breach || 0),
        })))
      }
      if (sm.rows?.[0]) setSummary(sm.rows[0])
      setLoading(false)
    }).catch(() => setLoading(false))
  }

  useEffect(loadData, [])

  const computeCompliance = async () => {
    setComputing(true)
    try {
      await fetch('/api/compliance/compute', { method: 'POST' })
      loadData()
    } catch(e) { alert('Error: ' + e.message) }
    setComputing(false)
  }

  const askGenie = () => {
    if (!genieSpaceId || !genieQuestion) return
    setGenieLoading(true)
    setGenieMessages(prev => [...prev, { role:'user', content: genieQuestion }])
    fetch('/api/genie/ask', {
      method:'POST', headers:{'Content-Type':'application/json'},
      body: JSON.stringify({ space_id: genieSpaceId, question: genieQuestion })
    }).then(r => r.json()).then(data => {
      if (data.conversation_id) {
        const poll = () => {
          fetch(`/api/genie/result/${genieSpaceId}/${data.conversation_id}/${data.message_id}`)
            .then(r => r.json()).then(result => {
              if (result.status === 'COMPLETED' || result.status === 'FAILED') {
                const text = result.result?.text || result.result?.description || 'No response'
                const query = result.result?.query
                let content = text
                if (query) content += '\n\nSQL:\n' + query
                setGenieMessages(prev => [...prev, { role:'assistant', content }])
                setGenieLoading(false)
              } else { setTimeout(poll, 2000) }
            })
        }
        setTimeout(poll, 3000)
      } else {
        setGenieMessages(prev => [...prev, { role:'assistant', content: JSON.stringify(data) }])
        setGenieLoading(false)
      }
    }).catch(e => {
      setGenieMessages(prev => [...prev, { role:'assistant', content: 'Error: ' + e.message }])
      setGenieLoading(false)
    })
    setGenieQuestion('')
  }

  const baselColor = summary?.basel_zone === 'GREEN' ? '#22c55e' : summary?.basel_zone === 'YELLOW' ? '#eab308' : '#ef4444'

  return (
    <div>
      <div className="page-header">
        <h1>05 - Compliance</h1>
        <p>Basel backtesting and interactive regulatory analysis</p>
      </div>

      {/* Basel Zones */}
      <div className="card">
        <div className="card-header">
          <div className="card-title">Basel Traffic Light System</div>
          <button className="btn btn-primary btn-sm" onClick={computeCompliance} disabled={computing}>
            {computing ? 'Computing...' : 'Compute Backtest'}
          </button>
        </div>
        <div className="grid-3">
          {[
            { level:'GREEN', threshold:'4 breaches or less', result:'No concern', color:'#22c55e' },
            { level:'YELLOW', threshold:'9 breaches or less', result:'Monitoring required', color:'#eab308' },
            { level:'RED', threshold:'10+ breaches', result:'VaR improvement needed', color:'#ef4444' },
          ].map(z => (
            <div key={z.level} style={{
              background: summary?.basel_zone === z.level ? `${z.color}15` : 'var(--bg-input)',
              border: `2px solid ${summary?.basel_zone === z.level ? z.color : 'var(--border-color)'}`,
              borderRadius:8, padding:20, textAlign:'center',
            }}>
              <div style={{fontSize:24,fontWeight:700,color:z.color}}>{z.level}</div>
              <div style={{fontSize:13,color:'var(--text-secondary)',margin:'4px 0'}}>{z.threshold}</div>
              <div style={{fontSize:12,color:'var(--text-muted)'}}>{z.result}</div>
            </div>
          ))}
        </div>
        {summary && (
          <div style={{marginTop:16,padding:'12px 16px',background:'var(--bg-input)',borderRadius:8,display:'flex',gap:32}}>
            <div><span style={{color:'var(--text-muted)',fontSize:12}}>Total Breaches:</span> <strong style={{color:baselColor}}>{summary.total_breaches}</strong></div>
            <div><span style={{color:'var(--text-muted)',fontSize:12}}>Total Days:</span> <strong>{summary.total_days}</strong></div>
            <div><span style={{color:'var(--text-muted)',fontSize:12}}>Breach Rate:</span> <strong>{summary.breach_pct}%</strong></div>
            <div><span style={{color:'var(--text-muted)',fontSize:12}}>Zone:</span> <strong style={{color:baselColor}}>{summary.basel_zone}</strong></div>
          </div>
        )}
      </div>

      {/* Backtest Chart */}
      {compData && (
        <div className="card">
          <div className="card-header">
            <div className="card-title">VaR99 Backtest</div>
            <div className="card-subtitle">{compData.length} trading days</div>
          </div>
          <ResponsiveContainer width="100%" height={350}>
            <LineChart data={compData}>
              <CartesianGrid strokeDasharray="3 3" stroke="#2a2a4a" />
              <XAxis dataKey="date" stroke="#606080" tick={{fontSize:11}} />
              <YAxis stroke="#606080" tick={{fontSize:11}} />
              <Tooltip contentStyle={{background:'#1a1a2e',border:'1px solid #2a2a4a',borderRadius:8}} labelStyle={{color:'#e8e8f0'}} />
              <Legend />
              <ReferenceLine y={0} stroke="#606080" strokeDasharray="3 3" />
              <Line type="monotone" dataKey="portfolio_return" stroke="#22c55e" strokeWidth={1.5} dot={false} name="Portfolio Return" />
              <Line type="monotone" dataKey="var_99" stroke="#ef4444" strokeWidth={2} dot={false} name="VaR99" strokeDasharray="5 5" />
            </LineChart>
          </ResponsiveContainer>
        </div>
      )}

      {/* Genie */}
      <div className="card">
        <div className="card-header">
          <div className="card-title">Genie - Interactive Analysis</div>
          <input className="form-input" style={{width:300}} placeholder="Genie Space ID"
            value={genieSpaceId} onChange={e=>setGenieSpaceId(e.target.value)} />
        </div>
        <div className="genie-chat">
          <div className="genie-messages">
            {genieMessages.length === 0 && (
              <div style={{textAlign:'center',color:'var(--text-muted)',padding:40,fontSize:14}}>
                Enter Genie Space ID and ask questions about your VaR data.
                <div style={{marginTop:12,display:'flex',gap:8,justifyContent:'center',flexWrap:'wrap'}}>
                  {['What is the worst VaR day?','Show breach count by month','Which country has highest risk?'].map(q =>
                    <button key={q} className="btn btn-outline btn-sm" onClick={()=>setGenieQuestion(q)}>{q}</button>
                  )}
                </div>
              </div>
            )}
            {genieMessages.map((msg,i) => (
              <div key={i} className={`genie-message ${msg.role}`}>
                <pre style={{whiteSpace:'pre-wrap',margin:0,fontFamily:'inherit',fontSize:'inherit'}}>{msg.content}</pre>
              </div>
            ))}
            {genieLoading && (
              <div className="genie-message assistant">
                <div className="spinner" style={{width:16,height:16,borderWidth:2,display:'inline-block',marginRight:8}} />Thinking...
              </div>
            )}
          </div>
          <div className="genie-input-row">
            <input className="form-input" placeholder={genieSpaceId?'Ask a question...':'Enter Genie Space ID first'}
              value={genieQuestion} onChange={e=>setGenieQuestion(e.target.value)}
              onKeyDown={e=>e.key==='Enter'&&askGenie()} disabled={!genieSpaceId} />
            <button className="btn btn-primary" onClick={askGenie} disabled={!genieSpaceId||!genieQuestion||genieLoading}>Send</button>
          </div>
        </div>
      </div>
    </div>
  )
}
