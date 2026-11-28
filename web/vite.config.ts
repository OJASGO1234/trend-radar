import { defineConfig } from "vite";
import react from "@vitejs/plugin-react";

// In dev, the browser talks to Vite (5173) only. Vite forwards /api/* to FastAPI (8000),
// so the React code just calls fetch("/api/...") and there is no CORS to configure.
export default defineConfig({
  plugins: [react()],
  server: {
    port: 5173,
    proxy: {
      "/api": { target: "http://127.0.0.1:8000", changeOrigin: true, timeout: 120_000 },
    },
  },
});
