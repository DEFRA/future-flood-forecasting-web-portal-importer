import { defineConfig } from 'vitest/config'

export default defineConfig({
  test: {
    coverage: {
      include: [
        '**/**',
        '!**/node_modules/**',
        '!**/testing/**',
        '!**/vendor/**',
        '!**/test.*'
      ],
      provider: 'istanbul',
      reporter: [
        'text',
        'lcov'
      ],
      reportsDirectory: 'testing/coverage',
      thresholds: {
        branches: 85,
        functions: 90,
        lines: 90,
        statements: 90
      }
    },
    environment: 'node',
    mockReset: true,
    restoreMocks: true
  }
})
