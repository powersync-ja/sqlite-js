import { defineConfig } from 'vitest/config';

export default defineConfig({
  esbuild: { target: 'es2022', exclude: /node/ },
  test: {
    environment: 'node',
    setupFiles: [] // Add any setup files if necessary
  },
  resolve: {
    alias: {
      'node:sqlite': 'node:sqlite'
    }
  }
});
