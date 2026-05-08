import { useState, useEffect } from 'react'
import { useNavigate } from 'react-router-dom'

const sections = [
  { number: '01', title: 'Data ETL', subtitle: 'Market Data Pipeline',
    desc: 'ポートフォリオ銘柄のダミー株式データを生成し、インタラクティブに時系列可視化',
    path: '/etl', color: '#06b6d4', bg: 'rgba(6,182,212,0.1)' },
  { number: '02', title: 'Model', subtitle: 'Risk Model Training',
    desc: 'マルチファクター回帰モデルの訓練、MLflow/Unity Catalog連携の確認',
    path: '/model', color: '#a855f7', bg: 'rgba(168,85,247,0.1)' },
  { number: '03', title: 'Monte Carlo', subtitle: 'Simulation Engine',
    desc: 'パラメータを調整しながら段階的にシミュレーション、分布の精緻化をリアルタイム可視化',
    path: '/montecarlo', color: '#f97316', bg: 'rgba(249,115,22,0.1)' },
  { number: '04', title: 'Aggregation', subtitle: 'VaR Dashboard',
    desc: 'SQL Warehouseによる集計結果とAI/BI Dashboard埋め込み表示',
    path: '/aggregation', color: '#22c55e', bg: 'rgba(34,197,94,0.1)' },
  { number: '05', title: 'Compliance', subtitle: 'Regulatory Report',
    desc: 'バーゼル準拠バックテスト、Genie APIでの対話的リスク分析',
    path: '/compliance', color: '#ef4444', bg: 'rgba(239,68,68,0.1)' },
]

export default function Home() {
  const nav = useNavigate()
  const [config, setConfig] = useState(null)
  useEffect(() => { fetch('/api/config').then(r => r.json()).then(setConfig).catch(() => {}) }, [])

  return (
    <>
      <div className="home-hero">
        <div className="home-hero-label">Databricks Lakehouse</div>
        <h1>RiskLens VaR</h1>
        <p>Delta Lake, MLflow, Apache Sparkを統合した<br/>次世代バリュー・アット・リスク分析プラットフォーム</p>
      </div>
      {config && (
        <div className="home-stats">
          <div className="home-stat">
            <div className="home-stat-value" style={{color:'var(--accent-blue)'}}>{config.portfolio_count}</div>
            <div className="home-stat-label">Stocks</div>
          </div>
          <div className="home-stat">
            <div className="home-stat-value" style={{color:'var(--accent-cyan)'}}>{config.indicator_count}</div>
            <div className="home-stat-label">Market Factors</div>
          </div>
          <div className="home-stat">
            <div className="home-stat-value" style={{color:'var(--accent-purple)'}}>{(config.monte_carlo?.runs||0).toLocaleString()}</div>
            <div className="home-stat-label">MC Trials</div>
          </div>
          <div className="home-stat">
            <div className="home-stat-value" style={{color:'var(--accent-green)',fontSize:16}}>{config.catalog}.{config.schema}</div>
            <div className="home-stat-label">Unity Catalog</div>
          </div>
        </div>
      )}
      <div className="home-grid">
        {sections.map(s => (
          <div key={s.number} className="home-card" onClick={() => nav(s.path)}>
            <div className="home-card-number">{s.number}</div>
            <div className="home-card-icon" style={{background:s.bg}}>
              <div style={{width:20,height:20,borderRadius:4,background:s.color}} />
            </div>
            <h3 style={{color:s.color}}>{s.title}</h3>
            <div style={{fontSize:12,color:'var(--text-muted)',marginBottom:8}}>{s.subtitle}</div>
            <p>{s.desc}</p>
          </div>
        ))}
      </div>
    </>
  )
}
