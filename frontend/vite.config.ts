/// <reference types="vitest/config" />
import { defineConfig } from "vitest/config";
import react from "@vitejs/plugin-react";

// The API base is read at runtime from VITE_API_BASE (see src/api/client.ts);
// CORS is permissive on the server in dev so the SPA can call it cross-origin.
export default defineConfig({
  plugins: [react()],
  server: { port: 5173 },
  test: {
    globals: true,
    environment: "jsdom",
    setupFiles: "./src/test/setup.ts",
  },
});
