# frozen_string_literal: true

class TigerHTTPClient
  class Response
    attr_reader :body

    def initialize(raw_response)
      @body = JSON.parse(raw_response.body)
      return unless @body["code"]
      return if GOOD_CODES.include?(@body["code"])
      raise TigerException.new @body["code"], @body["message"]
    end
  end
end
