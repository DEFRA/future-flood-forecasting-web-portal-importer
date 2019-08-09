const context = require('../testing/defaultContext')
const message = require('../testing/defaultMessage')

// Temporary dummy test to check if Jest is set up correctly.
test('Always pass', async () => {
  context.log(message)
  expect(true).toBe(true)
})
