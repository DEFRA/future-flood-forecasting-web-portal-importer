import { publishMessages } from '../../../Shared/service-bus-helper.js'
import { describe, expect, it } from 'vitest'

export const serviceBusHelperTests = () => describe('Test service bus helper', () => {
  describe('The Service Bus helper', () => {
    it('should propagate an error created during message publication', async () => {
      await expect(publishMessages()).rejects.toThrow()
    })
  })
})
