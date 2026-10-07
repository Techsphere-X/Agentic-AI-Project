// Stroke icons for the sidebar (24×24 grid, drawn in currentColor).
const PATHS = {
  dashboard: 'M3 3h7v9H3zM14 3h7v5h-7zM14 12h7v9h-7zM3 16h7v5H3z',
  catalog: 'M4 6c0-1.7 3.6-3 8-3s8 1.3 8 3-3.6 3-8 3-8-1.3-8-3zM4 6v6c0 1.7 3.6 3 8 3s8-1.3 8-3V6M4 12v6c0 1.7 3.6 3 8 3s8-1.3 8-3v-6',
  airflow: 'M4 7h16M4 12h16M4 17h16M7 4v16M17 4v16',
  database: 'M4 6c0-1.7 3.6-3 8-3s8 1.3 8 3-3.6 3-8 3-8-1.3-8-3zM4 6v6c0 1.7 3.6 3 8 3s8-1.3 8-3V6M4 12v6c0 1.7 3.6 3 8 3s8-1.3 8-3v-6',
  channel: 'M6 16V11a6 6 0 1 1 12 0v5l2 2H4zM10 20.5a2 2 0 0 0 4 0',
  workflows: 'M5 3h4v4H5zM15 10h4v4h-4zM5 17h4v4H5zM7 7v10M9 5h3a3 3 0 0 1 3 3v4M9 19h3a3 3 0 0 0 3-3v-2',
  runs: 'M12 21a9 9 0 1 0 0-18 9 9 0 0 0 0 18zM10 8.5v7l6-3.5z',
  approvals: 'M9 11l2.5 2.5L16 9M12 21a9 9 0 1 0 0-18 9 9 0 0 0 0 18z',
  incidents: 'M12 3l9.5 17h-19zM12 10v4M12 17.5v.5',
  notifications: 'M6 16V11a6 6 0 1 1 12 0v5l2 2H4zM10 20.5a2 2 0 0 0 4 0',
  analyzer: 'M4 5h16M4 9h10M4 13h6M15.5 18a3.5 3.5 0 1 0 0-7 3.5 3.5 0 0 0 0 7zM18 17.5l3 3',
  users: 'M9 11a4 4 0 1 0 0-8 4 4 0 0 0 0 8zM2 21v-1a6 6 0 0 1 6-6h2a6 6 0 0 1 6 6v1M16 3.5a4 4 0 0 1 0 7M18 14a5 5 0 0 1 4 5v2',
  collapse: 'M15 6l-6 6 6 6M20 4v16',
  expand: 'M9 6l6 6-6 6M4 4v16',
}

export default function NavIcon({ name }) {
  return (
    <svg
      className="nav-icon"
      viewBox="0 0 24 24"
      width="18"
      height="18"
      fill="none"
      stroke="currentColor"
      strokeWidth="1.8"
      strokeLinecap="round"
      strokeLinejoin="round"
      aria-hidden="true"
    >
      <path d={PATHS[name]} />
    </svg>
  )
}
