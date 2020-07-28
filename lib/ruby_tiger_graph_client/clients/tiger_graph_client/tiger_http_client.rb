# frozen_string_literal: true

require_relative '../response/response.rb'

class TigerGraphClient
  class TigerHTTPClient
    def initialize
      @httpclient = HTTPClient.new
    end

    def post(url, json)
      execute :post, url, json
    end

    def get(url)
      execute :get, url
    end

    def delete(url)
      execute :delete, url
    end

    def execute(command, *args)
      Response.new(fetch(command, *args))
    end

    def fetch(command, *args)
      @httpclient.send(command, *args)  
    end
  end
end
