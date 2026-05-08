import { useState, useEffect } from 'react'
import { LineChart, Line, XAxis, YAxis, CartesianGrid, Tooltip, ResponsiveContainer, Legend, ReferenceLine } from 'recharts'

export default function Compliance() {
  const [compData, setCompData] = useState(null)
  const [summary, setSummary] = useState(null)
  const [loading, setLoading] = useState(true)

  // Genie
  const [genieSpaceId, setGenieSpaceId] = useState('01f14ac99ca91fbf8bf29620b27b6e49')
  const [genieQuestion, setGenieQuestion] = useState('')
  const [genieMessages, setGenieMessages] = useState([])
  const [genieLoading, setGenieLoading] = useState(false)
  const [genieConvId, setGenieConvId] = useState(null)

  useEffect(() => {
    Promise.all([
      fetch('/api/compliance/data').then(r=>r.json()).catch(()=>({rows:[]})),
      fetch('/api/compliance/summary').then(r=>r.json()).catch(()=>({rows:[]})),
    ]).then(([cd, sm]) => {
      if (cd.rows?.length > 0) {
        setCompData(cd.rows.map(r => ({
          date: r.date?.substring(0,10),
          portfolio_return: parseFloat(r.portfolio_return || 0),
          var_99: parseFloat(r.var_99 || 0),
          is_breach: parseInt(r.is_breach || 0),
        })))
      }
      if (sm.rows?.[0]) setSummary(sm.rows[0])
      setLoading(false)
    })
  }, [])

  const askGenie = async () => {
    if (!genieSpaceId || !genieQuestion) return
    setGenieLoading(true)
    const question = genieQuestion
    setGenieMessages(prev => [...prev, { role:'user', content: question }])
    setGenieQuestion('')

    try {
      const resp = await fetch('/api/genie/ask', {
        method:'POST', headers:{'Content-Type':'application/json'},
        body: JSON.stringify({ space_id: genieSpaceId, question })
      })
      if (!resp.ok) { throw new Error(await resp.text()) }
      const data = await resp.json()

      if (data.conversation_id) {
        setGenieConvId(data.conversation_id)
        // Poll for result
        const poll = async () => {
          for (let i = 0; i < 30; i++) {
            await new Promise(r => setTimeout(r, 2000))
            try {
              const res = await fetch(`/api/genie/result/${genieSpaceId}/${data.conversation_id}/${data.message_id}`).then(r=>r.json())
              if (res.status === 'COMPLETED') {
                const text = res.result?.text || res.result?.description || 'Analysis complete'
                const query = res.result?.query
                let content = text
                if (query) content += '\n\n```sql\n' + query + '\n```'
                setGenieMessages(prev => [...prev, { role:'assistant', content }])
                setGenieLoading(false)
                return
              } else if (res.status === 'FAILED') {
                setGenieMessages(prev => [...prev, { role:'assistant', content: 'Failed: ' + (res.result?.text || 'Unknown error') }])
                setGenieLoading(false)
                return
              }
            } catch(e) {}
          }
          setGenieMessages(prev => [...prev, { role:'assistant', content: 'Timeout waiting for response' }])
          setGenieLoading(false)
        }
        poll()
      } else {
        setGenieMessages(prev => [...prev, { role:'assistant', content: JSON.stringify(data) }])
        setGenieLoading(false)
      }
    } catch(e) {
      setGenieMessages(prev => [...prev, { role:'assistant', content: 'Error: ' + e.message }])
      setGenieLoading(false)
    }
  }

  if (loading) return <div className="loading"><div className="spinner"/>Loading...</div>

  const baselColor = summary?.basel_zone === 'GREEN' ? '#22c55e' : summary?.basel_zone === 'YELLOW' ? '#eab308' : '#ef4444'

  // Compute monthly breaches for chart
  const monthlyBreaches = {}
  compData?.forEach(d => {
    const month = d.date?.substring(0,7)
    if (!monthlyBreaches[month]) monthlyBreaches[month] = { month, breaches: 0, days: 0 }
    monthlyBreaches[month].days++
    monthlyBreaches[month].breaches += d.is_breach
  })
  const monthlyData = Object.values(monthlyBreaches)

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
          <div style={{marginTop:16,padding:'12px 16px',background:'var(--bg-input)',borderRadius:8,display:'flex',gap:32,flexWrap:'wrap'}}>
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

      {/* Monthly Breach Count */}
      {monthlyData.length > 0 && (
        <div className="card">
          <div className="card-header">
            <div className="card-title">Monthly Breach Count</div>
          </div>
          <ResponsiveContainer width="100%" height={200}>
            <LineChart data={monthlyData}>
              <CartesianGrid strokeDasharray="3 3" stroke="#2a2a4a" />
              <XAxis dataKey="month" stroke="#606080" tick={{fontSize:11}} />
              <YAxis stroke="#606080" tick={{fontSize:11}} allowDecimals={false} />
              <Tooltip contentStyle={{background:'#1a1a2e',border:'1px solid #2a2a4a',borderRadius:8}} />
              <Line type="monotone" dataKey="breaches" stroke="#ef4444" strokeWidth={2} name="Breaches" />
            </LineChart>
          </ResponsiveContainer>
        </div>
      )}

      {/* Genie */}
      <div className="card">
        <div className="card-header">
          <div className="card-title">Genie - Interactive Risk Analysis</div>
          <input className="form-input" style={{width:280}} placeholder="Genie Space ID"
            value={genieSpaceId} onChange={e=>setGenieSpaceId(e.target.value)} />
        </div>
        <div className="genie-chat">
          <div className="genie-messages">
            {genieMessages.length === 0 && (
              <div style={{textAlign:'center',color:'var(--text-muted)',padding:32,fontSize:14}}>
                <div style={{marginBottom:8}}>Enter your Genie Space ID to start interactive analysis.</div>
                <div style={{fontSize:12,color:'var(--text-muted)',marginBottom:12}}>
                  The Space should have access to tables in <code style={{background:'var(--bg-input)',padding:'2px 6px',borderRadius:4}}>{genieSpaceId ? '' : 'skotani_var.var_app'}</code>
                </div>
                <div style={{display:'flex',gap:6,justifyContent:'center',flexWrap:'wrap'}}>
                  {[
                    'What is the worst VaR99 day and why?',
                    'Show monthly breach counts',
                    'Which dates had the largest portfolio losses?',
                    'Summarize the risk profile',
                  ].map(q => (
                    <button key={q} className="btn btn-outline btn-sm" onClick={()=>setGenieQuestion(q)}>{q}</button>
                  ))}
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
                <div className="spinner" style={{width:16,height:16,borderWidth:2,display:'inline-block',marginRight:8}} />Analyzing...
              </div>
            )}
          </div>
          <div className="genie-input-row">
            <input className="form-input" placeholder={genieSpaceId ? 'Ask about your VaR data...' : 'Enter Genie Space ID first'}
              value={genieQuestion} onChange={e=>setGenieQuestion(e.target.value)}
              onKeyDown={e=>e.key==='Enter'&&askGenie()} disabled={!genieSpaceId} />
            <button className="btn btn-primary" onClick={askGenie} disabled={!genieSpaceId||!genieQuestion||genieLoading}>Ask</button>
          </div>
        </div>
      </div>
    </div>
  )
}
