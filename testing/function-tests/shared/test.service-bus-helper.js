const { publishMessages } = require('../../../Shared/service-bus-helper')

module.exports = describe('Test service bus helper', () => {
  describe('The Service Bus helper', () => {
    it('should propagate an error created during message publication', async () => {
      await expect(publishMessages()).rejects.toThrow()
    })
  })
})
