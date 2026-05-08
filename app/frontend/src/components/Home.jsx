import { useState, useEffect } from 'react'
import { useNavigate } from 'react-router-dom'

const sections = [
  {
    number: '01',
    title: 'Data ETL',
    subtitle: 'Market Data Pipeline',
    desc: 'Yahoo Financeからの株式データ取得、変換処理、データリネージの可視化',
    path: '/etl',
    color: '#06b6d4',
    bg: 'rgba(6, 182, 212, 0.1)',
  },
  {
    number: '02',
    title: 'Model',
    subtitle: 'Risk Model Training',
    desc: 'MLflowで管理された予測モデルのメタデータ、バージョン、精度の確認',
    path: '/model',
    color: '#a855f7',
    bg: 'rgba(168, 85, 247, 0.1)',
  },
  {
    number: '03',
    title: 'Monte Carlo',
    subtitle: 'Simulation Engine',
    desc: 'シミュレーションパラメータの調整と結果の可視化、VaR分布の確認',
    path: '/montecarlo',
    color: '#f97316',
    bg: 'rgba(249, 115, 22, 0.1)',
  },
  {
    number: '04',
    title: 'Aggregation',
    subtitle: 'VaR Dashboard',
    desc: 'AI/BI Dashboardの埋め込み表示、国別・業種別のリスク集計',
    path: '/aggregation',
    color: '#22c55e',
    bg: 'rgba(34, 197, 94, 0.1)',
  },
  {
    number: '05',
    title: 'Compliance',
    subtitle: 'Regulatory Report',
    desc: 'バーゼル準拠のバックテスト、Genie APIでの対話的分析、Jobs証跡',
    path: '/compliance',
    color: '#ef4444',
    bg: 'rgba(239, 68, 68, 0.1)',
  },
]

export default function Home() {
  const navigate = useNavigate()
  const [config, setConfig] = useState(null)

  useEffect(() => {
    fetch('/api/config').then(r => r.json()).then(setConfig).catch(() => {})
  }, [])

  return (
    <>
      <div className="home-hero">
        <div className="home-hero-label">Databricks Lakehouse</div>
        <h1>RiskLens VaR</h1>
        <p>
          Delta Lake, MLflow, Apache Sparkを統合した<br/>
          次世代バリュー・アット・リスク分析プラットフォーム
        </p>
      </div>

      {config && (
        <div className="home-stats">
          <div className="home-stat">
            <div className="home-stat-value" style={{ color: 'var(--accent-blue)' }}>{config.portfolio_count}</div>
            <div className="home-stat-label">Stocks</div>
          </div>
          <div className="home-stat">
            <div className="home-stat-value" style={{ color: 'var(--accent-cyan)' }}>{config.indicator_count}</div>
            <div className="home-stat-label">Market Factors</div>
          </div>
          <div className="home-stat">
            <div className="home-stat-value" style={{ color: 'var(--accent-purple)' }}>{(config.monte_carlo.runs || 0).toLocaleString()}</div>
            <div className="home-stat-label">MC Trials</div>
          </div>
          <div className="home-stat">
            <div className="home-stat-value" style={{ color: 'var(--accent-green)', fontSize: 18 }}>{config.catalog}.{config.schema}</div>
            <div className="home-stat-label">Unity Catalog</div>
          </div>
        </div>
      )}

      <div className="home-grid">
        {sections.map(s => (
          <div
            key={s.number}
            className="home-card"
            onClick={() => navigate(s.path)}
          >
            <div className="home-card-number">{s.number}</div>
            <div className="home-card-icon" style={{ background: s.bg }}>
              <div style={{ width: 20, height: 20, borderRadius: 4, background: s.color }} />
            </div>
            <h3 style={{ color: s.color }}>{s.title}</h3>
            <div style={{ fontSize: 12, color: 'var(--text-muted)', marginBottom: 8 }}>{s.subtitle}</div>
            <p>{s.desc}</p>
          </div>
        ))}
      </div>
    </>
  )
}
