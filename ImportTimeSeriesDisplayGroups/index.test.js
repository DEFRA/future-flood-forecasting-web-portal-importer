const context = require('../testing/mocks/defaultContext')

// Temporary dummy test to check if Jest is set up correctly.
test('Always pass', async () => {
  context.log('This test always passes')
  expect(true).toBe(true)
})
