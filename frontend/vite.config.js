import react from '@vitejs/plugin-react'
import { defineConfig } from 'vite'

// Backend the dev proxy forwards /api to. Default = the usual local backend; the LAN start
// scripts (scripts/lan) set API_PROXY_TARGET so each instance talks to its own backend.
const apiTarget = process.env.API_PROXY_TARGET || 'http://127.0.0.1:8000'

// https://vite.dev/config/
export default defineConfig({
  plugins: [react()],
  // Separate dependency cache per instance so two dev servers don't clobber each other.
  cacheDir: process.env.VITE_CACHE_DIR || 'node_modules/.vite',
  server: {
    // Same-origin API in dev so the httpOnly refresh cookie (SameSite=Strict) is sent.
    // Browsers (including on other LAN machines) only ever talk to this port.
    proxy: {
      '/api': { target: apiTarget, changeOrigin: false },
    },
  },
})
