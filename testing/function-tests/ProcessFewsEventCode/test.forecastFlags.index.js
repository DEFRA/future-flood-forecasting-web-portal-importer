module.exports = describe('Test forecast flags', () => {
  const { isBoolean } = require('../../../Shared/utils')

  describe('Forecast flag testing ', () => {
    it('should return true for boolean values', () => {
      expect(isBoolean(true)).toBe(true)
      expect(isBoolean(false)).toBe(true)
    })
    it('should return true for boolean string values regardless of case', () => {
      expect(isBoolean('True')).toBe(true)
      expect(isBoolean('false')).toBe(true)
    })
    it('should return false for non-boolean values', () => {
      expect(isBoolean(0)).toBe(false)
      expect(isBoolean('string')).toBe(false)
    })
  })
})
