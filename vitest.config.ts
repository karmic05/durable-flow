import { defineConfig } from 'vitest/config';
export default defineConfig({
  test: {
    globalSetup: ['./packages/engine/test/global-setup.ts'],
    testTimeout: 60_000,
    hookTimeout: 120_000,
    include: ['packages/**/test/**/*.test.ts', 'examples/**/*.test.ts'],
    fileParallelism: false,
  },
});
