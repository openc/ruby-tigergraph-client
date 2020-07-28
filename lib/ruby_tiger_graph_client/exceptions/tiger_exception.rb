# frozen_string_literal: true

class TigerException < RuntimeError
  attr_reader :code

  def initialize(code, message)
    @code = code
    super(message)
  end
end
