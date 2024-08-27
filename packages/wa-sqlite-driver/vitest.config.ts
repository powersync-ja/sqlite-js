import { defineConfig } from 'vitest/config';
import wasm from 'vite-plugin-wasm';
import topLevelAwait from 'vite-plugin-top-level-await';

export default defineConfig({
  esbuild: { target: 'es2022' },
  plugins: [wasm()],
  optimizeDeps: {
    // Don't optimise these packages as they contain web workers and WASM files.
    // https://github.com/vitejs/vite/issues/11672#issuecomment-1415820673
    exclude: ['@journeyapps/wa-sqlite'],
    include: []
  },
  test: {
    // environment: 'node',
    // include: ['test/src/**/*.test.ts'],
    browser: {
      enabled: true,
      provider: 'webdriverio',
      name: 'chrome'
      // provider: 'playwright',
      // name: 'chromium'
    }
  }
});
