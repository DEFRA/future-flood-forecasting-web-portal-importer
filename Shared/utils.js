module.exports = {
  isBoolean: function (value) {
    if (typeof (value) === 'boolean') {
      return true
    } else if (typeof (value) === 'string') {
      return !!value.match(/true|false/i)
    } else {
      return false
    }
  }
}
