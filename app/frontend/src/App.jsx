import { Routes, Route, useNavigate, useLocation } from 'react-router-dom'
import Home from './components/Home'
import ETL from './components/ETL'
import Model from './components/Model'
import MonteCarlo from './components/MonteCarlo'
import Aggregation from './components/Aggregation'
import Compliance from './components/Compliance'

export default function App() {
  const navigate = useNavigate()
  const location = useLocation()
  const isHome = location.pathname === '/'

  return (
    <div className="app-container">
      {!isHome && (
        <header className="top-bar">
          <button className="back-btn" onClick={() => navigate('/')}>
            <span className="back-arrow">&#8592;</span>
            <span className="top-bar-logo">RiskLens VaR</span>
          </button>
          <nav className="top-nav">
            {[
              { path: '/etl', label: 'Data ETL', n: '01' },
              { path: '/model', label: 'Model', n: '02' },
              { path: '/montecarlo', label: 'Monte Carlo', n: '03' },
              { path: '/aggregation', label: 'Aggregation', n: '04' },
              { path: '/compliance', label: 'Compliance', n: '05' },
            ].map(item => (
              <button
                key={item.path}
                className={`top-nav-item ${location.pathname === item.path ? 'active' : ''}`}
                onClick={() => navigate(item.path)}
              >
                <span className="top-nav-number">{item.n}</span>
                {item.label}
              </button>
            ))}
          </nav>
        </header>
      )}
      <main className={isHome ? 'main-home' : 'main-page'}>
        <Routes>
          <Route path="/" element={<Home />} />
          <Route path="/etl" element={<ETL />} />
          <Route path="/model" element={<Model />} />
          <Route path="/montecarlo" element={<MonteCarlo />} />
          <Route path="/aggregation" element={<Aggregation />} />
          <Route path="/compliance" element={<Compliance />} />
        </Routes>
      </main>
    </div>
  )
}
